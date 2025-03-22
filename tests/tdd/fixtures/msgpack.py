from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import freezegun.api
import msgpack
import pytest


def encode(obj: Any) -> Any:
    if isinstance(obj, freezegun.api.FakeDatetime):
        return {"__fake_datetime__": obj.isoformat()}
    raise TypeError("Unsupported type")  # pragma: no cover


def decode(obj: Any) -> Any:
    if isinstance(obj, dict) and "__fake_datetime__" in obj:
        return freezegun.api.FakeDatetime.fromisoformat(obj["__fake_datetime__"])
    return obj


@pytest.fixture(autouse=True)
def patch_msgpack() -> Generator[None]:
    # Store original references once
    original_packb = msgpack.packb
    original_unpackb = msgpack.unpackb

    with (
        patch.object(msgpack, "packb") as patched_packb,
        patch.object(msgpack, "unpackb") as patched_unpackb,
    ):
        patched_packb.side_effect = lambda obj, **kwargs: original_packb(
            obj, default=encode, **kwargs
        )
        patched_unpackb.side_effect = lambda data, **kwargs: original_unpackb(
            data, object_hook=decode, **kwargs
        )

        yield
