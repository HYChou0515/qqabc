from __future__ import annotations

import datetime as dt
from typing import Any

import msgpack


def _encode(obj: Any) -> Any:
    if isinstance(obj, dt.datetime):
        return {"__datetime__": obj.isoformat()}
    raise TypeError(f"Unsupported type: {type(obj)}")  # pragma: no cover


def _decode(obj: Any) -> Any:
    if isinstance(obj, dict) and "__datetime__" in obj:
        return dt.datetime.fromisoformat(obj["__datetime__"])
    return obj


class _MsgpackSerializer:
    def packb(self, obj: Any) -> bytes:
        return msgpack.packb(obj, default=_encode)

    def unpackb(self, raw: bytes) -> Any:
        return msgpack.unpackb(raw, object_hook=_decode)


serializer = _MsgpackSerializer()
