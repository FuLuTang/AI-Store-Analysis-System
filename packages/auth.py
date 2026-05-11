import hashlib
import secrets
import time
from collections import deque
from dataclasses import dataclass, field
from threading import Lock
from typing import Deque, Dict


def generate_user_key() -> str:
    return f"fzt_{secrets.token_urlsafe(24)}"


def hash_user_key(user_key: str) -> str:
    return hashlib.sha256(user_key.encode("utf-8")).hexdigest()


def mask_user_key(user_key: str) -> str:
    if len(user_key) <= 10:
        return "fzt_***"
    return f"{user_key[:6]}...{user_key[-4:]}"


@dataclass
class RegisterRateLimiter:
    window_seconds: int = 180
    max_requests: int = 5
    _hits: Dict[str, Deque[float]] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock)

    def allow(self, identity: str) -> bool:
        now = time.time()
        with self._lock:
            bucket = self._hits.setdefault(identity, deque())
            while bucket and now - bucket[0] > self.window_seconds:
                bucket.popleft()
            if len(bucket) >= self.max_requests:
                return False
            bucket.append(now)
            return True
