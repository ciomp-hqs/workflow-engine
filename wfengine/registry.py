"""Registries for node types and business functions.

Node type contract (see README "节点接口"):

    @register_node("my_type")
    class MyNode(NodeType):
        def validate(self, node_cfg):      # optional, called at workflow load time
            return []                      # list of error strings
        def run(self, ctx):                # ctx: wfengine.engine.NodeContext
            return {"any": "json"}         # output dict -> nodes.<id>.output

A plain function ``def handler(ctx) -> dict`` can also be registered.
Functions used by the ``function`` node type are registered with
``@register_function("name")`` and are called as ``fn(inputs, ctx)``.
"""
import threading


class NodeType:
    """Base class for node types. Subclasses override ``run`` (and optionally ``validate``)."""

    #: default for node-level ``idempotent`` when the config does not set it
    default_idempotent = False

    def validate(self, node_cfg):
        return []

    def run(self, ctx):  # pragma: no cover - abstract
        raise NotImplementedError


class _FuncNodeType(NodeType):
    def __init__(self, fn):
        self._fn = fn

    def run(self, ctx):
        return self._fn(ctx)


class Registry:
    def __init__(self, kind):
        self.kind = kind
        self._items = {}
        self._lock = threading.Lock()

    def register(self, name, obj, replace=False):
        with self._lock:
            if name in self._items and not replace and self._items[name] is not obj:
                raise ValueError("%s %r already registered" % (self.kind, name))
            self._items[name] = obj
        return obj

    def get(self, name):
        return self._items.get(name)

    def __contains__(self, name):
        return name in self._items

    def names(self):
        return sorted(self._items)

    def unregister(self, name):
        with self._lock:
            self._items.pop(name, None)


node_types = Registry("node type")
functions = Registry("function")


def register_node(name, replace=False):
    """Decorator registering a NodeType subclass or a ``handler(ctx)`` function."""

    def deco(obj):
        if isinstance(obj, type) and issubclass(obj, NodeType):
            inst = obj()
        elif isinstance(obj, NodeType):
            inst = obj
        elif callable(obj):
            inst = _FuncNodeType(obj)
        else:
            raise TypeError("cannot register %r as node type" % (obj,))
        node_types.register(name, inst, replace=replace)
        return obj

    return deco


def register_function(name, replace=False):
    """Decorator registering ``fn(inputs: dict, ctx) -> dict`` for the ``function`` node type."""

    def deco(fn):
        functions.register(name, fn, replace=replace)
        return fn

    return deco
