"""Local deterministic mock model service (no network, no keys).

Tasks (request: {"text": str, ...}; response: dict)
  extract_points    split text into sentences on 。！？.!? and newlines; keep sentences with
                    >= 4 chars; return the first ``max_points`` (default 3), each cut to 60 chars.
                    -> {"points": [...], "model": "mock-llm-v1"}
  sensitive_check   keyword + regex rules (see SENSITIVE_KEYWORDS / SENSITIVE_PATTERNS).
                    -> {"sensitive": bool, "findings": [{"type", "match"(masked), "rule"}], "model": ...}
  summarize         first 2 points joined by "；" (or the first 80 chars of text).

Failure injection (for tests / demos)
  * config ``fail_first_n: {"extract_points": 2}``: the first 2 calls of that task raise
    MockLLMError (a retryable error), later calls succeed. Counter is per service instance.
  * config ``always_fail: ["sensitive_check"]``: every call of that task fails.
  * text containing ``[[MOCK_FAIL:<task>]]``: every call of <task> on that text fails.
  * config ``latency_seconds``: sleep before answering (cancellation is checked while sleeping).
The same service can be exposed over HTTP (``python -m wfengine mock-llm --port 8765``) and
used via ``llm.mode = "http"`` in config; tests use the in-process mode.
"""
import json
import re
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .errors import NodeError

MODEL_NAME = "mock-llm-v1"

SENSITIVE_KEYWORDS = {
    "身份证": "id_card_keyword",
    "密码": "password",
    "password": "password",
    "银行卡": "bank_card",
    "机密": "confidential",
    "confidential": "confidential",
    "内部绝密": "confidential",
    "salary": "salary",
    "薪资": "salary",
}
SENSITIVE_PATTERNS = [
    ("phone_number", re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")),
    ("id_number", re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")),
]
SENTENCE_SPLIT = re.compile(r"[。！？!?\n]+|(?<=[A-Za-z0-9])\.(?=\s|$)")
FAIL_MARKER = re.compile(r"\[\[MOCK_FAIL:([a-z_]+)\]\]")


class MockLLMError(NodeError):
    """Injected / simulated model failure (retryable)."""


def mask(s):
    if len(s) <= 5:
        return "*" * len(s)
    return s[:3] + "*" * (len(s) - 5) + s[-2:]


def split_sentences(text):
    return [s.strip() for s in SENTENCE_SPLIT.split(text or "") if len(s.strip()) >= 4]


class MockLLMService:
    TASKS = ("extract_points", "sensitive_check", "summarize")

    def __init__(self, fail_first_n=None, always_fail=None, latency_seconds=0.0):
        self.fail_first_n = dict(fail_first_n or {})
        self.always_fail = set(always_fail or [])
        self.latency_seconds = float(latency_seconds or 0)
        self.calls = {}
        self._lock = threading.Lock()

    def complete(self, task, payload, check_cancelled=None):
        if task not in self.TASKS:
            raise MockLLMError("mock-llm: unknown task %r" % task)
        with self._lock:
            n = self.calls.get(task, 0) + 1
            self.calls[task] = n
        if self.latency_seconds:
            end = time.time() + self.latency_seconds
            while time.time() < end:
                if check_cancelled:
                    check_cancelled()
                time.sleep(min(0.02, max(0, end - time.time())))
        text = str(payload.get("text") or "")
        if task in self.always_fail or task in FAIL_MARKER.findall(text):
            raise MockLLMError("mock-llm: injected failure for task %s (call #%d)" % (task, n))
        if n <= int(self.fail_first_n.get(task, 0)):
            raise MockLLMError("mock-llm: injected transient failure for task %s (call #%d of fail_first_n=%d)"
                               % (task, n, self.fail_first_n[task]))
        return getattr(self, "_" + task)(text, payload)

    def _extract_points(self, text, payload):
        k = int(payload.get("max_points") or 3)
        pts = [s[:60] for s in split_sentences(text)[:k]]
        return {"points": pts, "model": MODEL_NAME}

    def _sensitive_check(self, text, payload):
        findings, seen = [], set()
        lower = text.lower()
        for kw, rule in SENSITIVE_KEYWORDS.items():
            if kw.lower() in lower and (rule, kw) not in seen:
                seen.add((rule, kw))
                findings.append({"type": "keyword", "match": kw, "rule": rule})
        for rule, pat in SENSITIVE_PATTERNS:
            for m in pat.findall(text):
                if (rule, m) not in seen:
                    seen.add((rule, m))
                    findings.append({"type": "pattern", "match": mask(m), "rule": rule})
        return {"sensitive": bool(findings), "findings": findings, "model": MODEL_NAME}

    def _summarize(self, text, payload):
        pts = payload.get("points") or split_sentences(text)
        summary = "；".join(pts[:2]) if pts else text[:80]
        return {"summary": summary, "model": MODEL_NAME}


class HTTPLLMClient:
    """Client for the mock service exposed over HTTP (same contract as MockLLMService.complete)."""

    def __init__(self, base_url, timeout=10):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def complete(self, task, payload, check_cancelled=None):
        req = urllib.request.Request(self.base_url + "/v1/tasks/" + task,
                                     data=json.dumps(payload).encode("utf-8"),
                                     headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise MockLLMError("mock-llm http %d: %s" % (e.code, e.read().decode("utf-8", "replace")))
        except (urllib.error.URLError, OSError) as e:
            raise MockLLMError("mock-llm unreachable: %s" % e)


def make_http_server(service, host="127.0.0.1", port=8765):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body):
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/health":
                self._send(200, {"ok": True, "model": MODEL_NAME})
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            if not self.path.startswith("/v1/tasks/"):
                return self._send(404, {"error": "not found"})
            task = self.path[len("/v1/tasks/"):]
            try:
                n = int(self.headers.get("Content-Length") or 0)
                payload = json.loads(self.rfile.read(n).decode("utf-8") or "{}")
                self._send(200, service.complete(task, payload))
            except MockLLMError as e:
                self._send(503, {"error": str(e)})
            except ValueError as e:
                self._send(400, {"error": str(e)})

    return ThreadingHTTPServer((host, port), Handler)


def build_llm_client(cfg):
    cfg = cfg or {}
    if cfg.get("mode") == "http":
        return HTTPLLMClient(cfg.get("base_url", "http://127.0.0.1:8765"))
    return MockLLMService(fail_first_n=cfg.get("fail_first_n"), always_fail=cfg.get("always_fail"),
                          latency_seconds=cfg.get("latency_seconds", 0))
