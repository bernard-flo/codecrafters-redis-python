import time
from dataclasses import dataclass
from typing import Generic, TypeVar


def current_milli_time() -> float:
    return time.time() * 1000


T = TypeVar("T")


@dataclass
class ExpiringEntry(Generic[T]):
    value: T
    expire_at: float | None


K = TypeVar("K")
V = TypeVar("V")


class ExpiringDict(Generic[K, V]):

    def __init__(self) -> None:
        self._dict: dict[K, ExpiringEntry[V]] = dict()

    def put(self, key: K, value: V, expiry_ms: float | None) -> None:
        self._dict[key] = ExpiringEntry(value, current_milli_time() + expiry_ms if expiry_ms else None)

    def get(self, key: K) -> V | None:
        entry = self._dict.get(key)
        if not entry:
            return None
        if entry.expire_at is None:
            return entry.value
        if entry.expire_at < current_milli_time():
            return None
        return entry.value
