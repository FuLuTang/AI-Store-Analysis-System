import hashlib
import json
import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Deque, Dict, Optional
from collections import deque

import bcrypt


USERNAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,63}$")
TOKEN_RANDOM_BYTES = 24
USER_SOFT_TTL_SECONDS = 2 * 60 * 60
USER_HARD_TTL_SECONDS = 12 * 60 * 60
SERVICE_SOFT_TTL_SECONDS = 30 * 60
SERVICE_HARD_TTL_SECONDS = 2 * 60 * 60


@dataclass(frozen=True)
class AccountRef:
    username: str
    username_hash: str
    account_id: str
    account_dir: Path


@dataclass(frozen=True)
class TokenCheckResult:
    account: AccountRef
    token_hash: str
    token_type: str
    parent_token_hash: Optional[str] = None

    @property
    def is_service(self) -> bool:
        return self.token_type == "service"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_time(value: str) -> float:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def normalize_username(username: str) -> str:
    value = (username or "").strip()
    if not USERNAME_RE.match(value):
        raise ValueError("账号名必须为 3-64 位字母、数字、下划线或短横线，且以字母或数字开头")
    return value


def username_hash(username: str) -> str:
    return hashlib.sha256(username.encode("utf-8")).hexdigest()


def account_id_for_username(username: str) -> str:
    normalized = normalize_username(username)
    return f"{normalized[:3]}_{username_hash(normalized)[:6]}"


def mask_username(username: str) -> str:
    normalized = normalize_username(username)
    return f"{normalized[:3]}***"


def account_ref_for_username(accounts_dir: Path, username: str) -> AccountRef:
    normalized = normalize_username(username)
    digest = username_hash(normalized)
    account_id = f"{normalized[:3]}_{digest[:6]}"
    return AccountRef(
        username=normalized,
        username_hash=digest,
        account_id=account_id,
        account_dir=accounts_dir / account_id,
    )


def account_ref_from_token(accounts_dir: Path, token: str) -> Optional[AccountRef]:
    raw = (token or "").strip()
    if raw.startswith("serv_"):
        body = raw[len("serv_"):]
        if len(body) < 11 or body[3] != "_":
            return None
        prefix, digest_prefix = body[:3], body[4:10]
    else:
        if len(raw) < 11 or raw[3] != "_":
            return None
        prefix, digest_prefix = raw[:3], raw[4:10]
    if not prefix or not re.match(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,2}$", prefix):
        return None
    if not re.match(r"^[a-f0-9]{6}$", digest_prefix):
        return None
    account_id = f"{prefix}_{digest_prefix}"
    account_dir = accounts_dir / account_id
    account_json = account_dir / "account.json"
    if not account_json.exists():
        return None
    try:
        data = json.loads(account_json.read_text(encoding="utf-8"))
    except Exception:
        return None
    username = str(data.get("username") or "").strip()
    digest = str(data.get("usernameHash") or "").strip()
    if not username or not digest:
        return None
    if account_id_for_username(username) != account_id or username_hash(username) != digest:
        return None
    return AccountRef(username=username, username_hash=digest, account_id=account_id, account_dir=account_dir)


def hash_password(password: str) -> str:
    raw = (password or "").encode("utf-8")
    return bcrypt.hashpw(raw, bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    if not password or not password_hash:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


def token_hash(token: str) -> str:
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()


def generate_user_token(account: AccountRef) -> str:
    random_part = secrets.token_urlsafe(TOKEN_RANDOM_BYTES)
    return f"{account.account_id}_{random_part}"


def generate_service_token(account: AccountRef) -> str:
    random_part = secrets.token_urlsafe(TOKEN_RANDOM_BYTES)
    return f"serv_{account.account_id}_{random_part}"


def token_log_path(account_dir: Path) -> Path:
    return account_dir / "account_tokens.jsonl"


def append_token_event(account_dir: Path, event: dict):
    account_dir.mkdir(parents=True, exist_ok=True)
    payload = {"time": utc_now_iso(), **event}
    with token_log_path(account_dir).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_token_events(account_dir: Path) -> list[dict]:
    path = token_log_path(account_dir)
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            if isinstance(data, dict):
                events.append(data)
        except Exception:
            continue
    return events


def cleanup_token_events(account_dir: Path, target_hash: str):
    events = read_token_events(account_dir)
    kept = [
        event for event in events
        if event.get("token_hash") != target_hash and event.get("parent_token_hash") != target_hash
    ]
    path = token_log_path(account_dir)
    if kept:
        path.write_text(
            "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in kept),
            encoding="utf-8",
        )
    elif path.exists():
        path.write_text("", encoding="utf-8")


def check_token_logable(accounts_dir: Path, token: str) -> Optional[TokenCheckResult]:
    account = account_ref_from_token(accounts_dir, token)
    if not account:
        return None

    raw = (token or "").strip()
    hashed = token_hash(raw)
    is_service = raw.startswith("serv_")
    soft_ttl = SERVICE_SOFT_TTL_SECONDS if is_service else USER_SOFT_TTL_SECONDS
    hard_ttl = SERVICE_HARD_TTL_SECONDS if is_service else USER_HARD_TTL_SECONDS
    creation_action = "serv_creation" if is_service else "creation"

    events = read_token_events(account.account_dir)
    latest = None
    creation = None
    for event in reversed(events):
        if event.get("token_hash") != hashed:
            continue
        if latest is None:
            latest = event
        if event.get("action") == creation_action:
            creation = event
            break

    if not latest or not creation:
        return None
    if latest.get("action") == "revoke":
        return None

    now = time.time()
    creation_time = parse_time(str(creation.get("time") or ""))
    if not creation_time or now - creation_time > hard_ttl:
        cleanup_token_events(account.account_dir, hashed)
        return None

    latest_time = parse_time(str(latest.get("time") or ""))
    if not latest_time or now - latest_time > soft_ttl:
        return None

    append_token_event(account.account_dir, {"action": "active", "token_hash": hashed})
    return TokenCheckResult(
        account=account,
        token_hash=hashed,
        token_type="service" if is_service else "user",
        parent_token_hash=creation.get("parent_token_hash"),
    )


class RegisterRateLimiter:
    def __init__(self, window_seconds: int = 180, max_requests: int = 5):
        self.window_seconds = window_seconds
        self.max_requests = max_requests
        self._hits: Dict[str, Deque[float]] = {}
        self._lock = Lock()

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
