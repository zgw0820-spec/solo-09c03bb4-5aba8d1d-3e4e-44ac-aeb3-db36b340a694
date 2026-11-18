"""
机房虚拟机启动器 · 内网 IPv4 地址租约服务

- FastAPI + SQLite(WAL)。Docker Compose 启动两个实例，挂载同一命名卷，共享同一个数据库文件。
- 申请幂等：request_id（申请号）唯一；同号重试返回原租约的当前状态，过期后也不会借旧号再领地址。
- 分配原子性：所有写操作走 BEGIN IMMEDIATE 事务，跨实例串行化；
  “查空闲地址 + 写租约” 是原子操作，两个实例争抢最后一个地址时只有一方成功。
- 懒过期：expires_at <= now 即视为过期，地址立即可回收分配给其它虚拟机，无需后台清理线程。
- 令牌保护：续租/释放必须携带租约令牌；地址易主后，旧令牌迟到的续租/释放不影响新归属。
- 同一虚拟机在任意时刻只能持有一个有效（未释放且未过期）地址。
- 网卡占用快照对账：提交递增序号 + 占用列表，对账列出仍占用已结束租约地址的虚拟机、
  同一地址多机占用、与有效租约不符的归属。对账只读租约台账，不改写任何租约。
- 快照即事实：过期/已释放地址只要仍出现在最新已提交快照中，就不会被再次分配，
  直到更新的快照确认其空闲。快照与申请在同一 BEGIN IMMEDIATE 事务体系内串行化，
  分配与对账永远基于一致的已提交状态；快照序号持久化，重启后继续使用最后成功提交的序号。
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import secrets
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

# ---------------- 配置（环境变量） ----------------

DB_PATH = os.environ.get("DB_PATH", "./data/leases.db")
POOL_CIDR = os.environ.get("POOL_CIDR", "10.99.0.0/29")
INSTANCE_ID = os.environ.get("INSTANCE_ID", "standalone")
ALLOW_RESET = os.environ.get("ALLOW_RESET", "0") == "1"  # 仅自测用途
MIN_TTL = 1
MAX_TTL = 60
BUSY_TIMEOUT_MS = 15000


def _load_pool(cidr: str) -> list[str]:
    net = ipaddress.ip_network(cidr, strict=False)
    addrs = [str(ip) for ip in net.hosts()]
    if not addrs:  # /32 的边界情况
        addrs = [str(net.network_address)]
    return addrs


POOL_ADDRESSES = _load_pool(POOL_CIDR)

# ---------------- SQLite ----------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS leases (
    lease_id    TEXT PRIMARY KEY,        -- 租约 ID
    request_id  TEXT NOT NULL UNIQUE,    -- 申请号（幂等键）：一个申请号终身只对应一条租约
    vm_id       TEXT NOT NULL,           -- 虚拟机 ID
    address     TEXT NOT NULL,           -- 分到的 IPv4 地址
    token       TEXT NOT NULL,           -- 租约令牌：续租/释放凭证
    issued_at   REAL NOT NULL,           -- 签发时间（epoch 秒）
    expires_at  REAL NOT NULL,           -- 过期时间（epoch 秒）
    released_at REAL,                    -- 释放时间；NULL = 未释放
    node        TEXT NOT NULL            -- 处理该申请的实例
);
CREATE INDEX IF NOT EXISTS idx_leases_address ON leases(address);
CREATE INDEX IF NOT EXISTS idx_leases_vm_id  ON leases(vm_id);

CREATE TABLE IF NOT EXISTS request_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,            -- 申请号
    vm_id      TEXT NOT NULL,
    result     TEXT NOT NULL,            -- leased / replayed / rejected_*
    lease_id   TEXT,
    detail     TEXT,
    node       TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_request_log_req ON request_log(request_id);

-- 虚拟化平台网卡占用快照（整份快照按递增序号提交，全部写操作与租约共用同一写事务体系）
CREATE TABLE IF NOT EXISTS snapshots (
    seq            INTEGER PRIMARY KEY,   -- 递增序号（单调递增，由提交方保证）
    content_hash   TEXT NOT NULL,         -- 规范化占用列表的哈希：同序号同内容幂等、同序号异内容冲突
    entry_count    INTEGER NOT NULL,
    committed_at   REAL NOT NULL,
    committed_node TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_entries (
    seq       INTEGER NOT NULL,           -- 所属快照序号
    vm_id     TEXT NOT NULL,             -- 占用该地址的虚拟机 ID
    address   TEXT NOT NULL,             -- 池内 IPv4 地址
    ord       INTEGER NOT NULL,          -- 提交时的原始顺序
    PRIMARY KEY (seq, vm_id, address),
    FOREIGN KEY (seq) REFERENCES snapshots(seq)
);
CREATE INDEX IF NOT EXISTS idx_snap_entries_seq ON snapshot_entries(seq);
CREATE INDEX IF NOT EXISTS idx_snap_entries_addr ON snapshot_entries(seq, address);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")  # 先设忙等，再开 WAL
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """BEGIN IMMEDIATE 写事务。

    两个实例共用同一个 SQLite 文件，BEGIN IMMEDIATE 立即取得写锁，
    把全部写事务在数据库层串行化，保证分配决策的原子性。
    """
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


@contextmanager
def read_conn() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    with read_conn() as conn:
        conn.executescript(SCHEMA)  # IF NOT EXISTS，两个实例并发启动也安全


# ---------------- 视图与工具 ----------------

def _iso(ts: Optional[float]) -> Optional[str]:
    return None if ts is None else datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _status_of(row: sqlite3.Row, now: float) -> str:
    if row["released_at"] is not None:
        return "released"
    if row["expires_at"] <= now:
        return "expired"
    return "active"


def _lease_view(row: sqlite3.Row, now: Optional[float] = None) -> dict[str, Any]:
    now = time.time() if now is None else now
    status = _status_of(row, now)
    return {
        "lease_id": row["lease_id"],
        "request_id": row["request_id"],
        "vm_id": row["vm_id"],
        "address": row["address"],
        "token": row["token"],
        "status": status,
        "issued_at": _iso(row["issued_at"]),
        "expires_at": _iso(row["expires_at"]),
        "released_at": _iso(row["released_at"]),
        "remaining_seconds": round(max(0.0, row["expires_at"] - now), 3) if status == "active" else 0,
        "leased_via_node": row["node"],
    }


def _error(status_code: int, code: str, message: str, **extra: Any) -> HTTPException:
    return HTTPException(status_code=status_code, detail={"error": code, "message": message, **extra})


def _conflict(code: str, message: str, **extra: Any) -> HTTPException:
    return _error(409, code, message, **extra)


def _log_request(conn: sqlite3.Connection, body: "ApplyIn", result: str,
                 lease_id: Optional[str], detail: str) -> None:
    conn.execute(
        "INSERT INTO request_log (request_id, vm_id, result, lease_id, detail, node, created_at)"
        " VALUES (?,?,?,?,?,?,?)",
        (body.request_id, body.vm_id, result, lease_id, detail, INSTANCE_ID, time.time()),
    )


# ---------------- 应用 ----------------

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="机房虚拟机地址租约服务",
    version="1.1.0",
    description=(
        "内网 IPv4 地址池租约 API。申请幂等（同申请号重试返回原租约当前状态）；"
        "同一虚拟机仅可持有一个有效地址；过期地址可回收再分配；"
        "续租/释放需携带租约令牌，旧令牌不影响新归属。"
        "新增网卡占用快照对账：递增序号提交整份占用，同序号同内容幂等、异内容或旧序号拒绝；"
        "快照中仍占用的已结束租约地址不会被再次分配，直到新快照确认空闲。"
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def add_instance_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Served-By"] = INSTANCE_ID
    return response


class ApplyIn(BaseModel):
    """启动器提交的申请：虚拟机 ID + 申请号 + 租期。"""

    vm_id: str = Field(min_length=1, max_length=128, description="虚拟机 ID", examples=["vm-1001"])
    request_id: str = Field(min_length=1, max_length=128,
                            description="申请号（幂等键）：同号重试返回原租约当前状态", examples=["req-9f3a2c"])
    ttl_seconds: int = Field(ge=MIN_TTL, le=MAX_TTL, description="租期（秒），1~60", examples=[30])


class RenewIn(BaseModel):
    token: str = Field(min_length=1, max_length=128, description="申请时返回的租约令牌")
    ttl_seconds: int = Field(ge=MIN_TTL, le=MAX_TTL, description="从当前时刻起续租的秒数，1~60", examples=[30])


class ReleaseIn(BaseModel):
    token: str = Field(min_length=1, max_length=128, description="申请时返回的租约令牌")


# ---------------- 网卡占用快照 ----------------

class SnapshotEntryIn(BaseModel):
    """快照中的一条网卡占用：虚拟机 ID + 池内 IPv4。"""

    vm_id: str = Field(min_length=1, max_length=128, description="占用该地址的虚拟机 ID",
                       examples=["vm-1001"])
    address: str = Field(min_length=1, max_length=64,
                         description="池内 IPv4 地址（须属于配置的地址池）", examples=["10.99.0.1"])


class SnapshotIn(BaseModel):
    """机房运维提交的整份网卡占用快照：递增序号 + 全量占用列表。"""

    seq: int = Field(ge=1, description="递增序号：只接受比已提交序号更大的值；同序号同内容幂等",
                     examples=[1])
    entries: list[SnapshotEntryIn] = Field(
        description="整份占用列表（全量，非增量）；同一地址出现多次即表示多机占用",
        examples=[[{"vm_id": "vm-1001", "address": "10.99.0.1"}]])

    @model_validator(mode="after")
    def _validate_addresses(self) -> "SnapshotIn":
        bad = sorted({e.address for e in self.entries if e.address not in POOL_ADDRESSES})
        if bad:
            raise ValueError(f"地址不在地址池 {POOL_CIDR} 内：{', '.join(bad)}")
        return self


def _canonical_entries(entries: list[SnapshotEntryIn]) -> list[tuple[str, str]]:
    """规范化为去重后的 (vm_id, address) 列表（按原始顺序去重，保持提交顺序）。

    完全重复的 (vm, address) 对视为幂等重复；同一地址被不同虚拟机占用则保留，
    作为“同一地址多机占用”在对账中报告。
    """
    seen: set[tuple[str, str]] = set()
    ordered: list[tuple[str, str]] = []
    for e in entries:
        pair = (e.vm_id, e.address)
        if pair not in seen:
            seen.add(pair)
            ordered.append(pair)
    return ordered


def _content_hash(pairs: list[tuple[str, str]]) -> str:
    payload = json.dumps(pairs, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _latest_snapshot_meta(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM snapshots ORDER BY seq DESC LIMIT 1").fetchone()


def _reconcile(conn: sqlite3.Connection, now: Optional[float] = None) -> dict[str, Any]:
    """基于最新已提交快照与租约台账计算对账结果（纯读，不改写任何租约）。

    三类差异：
    - stale_lease_occupancy：地址的最新租约已结束（过期/释放），但快照中仍有虚拟机占用；
    - duplicate_occupancy：同一地址在快照中被多台虚拟机占用；
    - unexpected_occupancy：快照占用与有效租约不符（无有效租约 / 归属虚拟机不同）。
    另附 missing_from_snapshot：有效租约在租但未出现在快照中（快照漏报）。
    """
    now = time.time() if now is None else now
    meta = _latest_snapshot_meta(conn)
    if meta is None:
        return {
            "snapshot_seq": None,
            "committed_at": None,
            "summary": {"entries": 0, "stale_lease_occupancy": 0, "duplicate_occupancy": 0,
                        "unexpected_occupancy": 0, "consistent": 0, "missing_from_snapshot": 0},
            "stale_lease_occupancy": [],
            "duplicate_occupancy": [],
            "unexpected_occupancy": [],
            "consistent": [],
            "missing_from_snapshot": [],
        }
    seq = meta["seq"]
    rows = conn.execute(
        "SELECT vm_id, address FROM snapshot_entries WHERE seq = ? ORDER BY ord", (seq,)).fetchall()

    # 地址 -> 快照中的占用虚拟机列表（保留重复占用）
    occupiers: dict[str, list[str]] = {}
    for r in rows:
        occupiers.setdefault(r["address"], []).append(r["vm_id"])

    # 一次性取相关地址的租约历史（按地址分组，签发时间倒序），避免逐条查询
    addresses = sorted(occupiers)
    placeholders = ",".join("?" for _ in addresses)
    lease_rows: list[sqlite3.Row] = []
    if addresses:
        lease_rows = conn.execute(
            f"SELECT * FROM leases WHERE address IN ({placeholders}) ORDER BY issued_at DESC, lease_id DESC",
            addresses).fetchall()
    history: dict[str, list[sqlite3.Row]] = {}
    for lr in lease_rows:
        history.setdefault(lr["address"], []).append(lr)

    stale: list[dict[str, Any]] = []
    duplicate: list[dict[str, Any]] = []
    unexpected: list[dict[str, Any]] = []
    consistent: list[dict[str, Any]] = []

    for addr, vms in occupiers.items():
        hist = history.get(addr, [])
        latest = hist[0] if hist else None
        latest_view = _lease_view(latest, now) if latest is not None else None
        active = latest if (latest is not None and _status_of(latest, now) == "active") else None
        is_duplicate = len(vms) > 1

        if is_duplicate:
            duplicate.append({
                "address": addr,
                "occupying_vms": vms,
                "active_lease": _lease_view(active, now) if active is not None else None,
                "latest_lease": latest_view,
                "message": f"地址 {addr} 在快照 #{seq} 中被 {len(vms)} 台虚拟机同时占用",
            })

        for vm in vms:
            entry = {"address": addr, "vm_id": vm}
            if latest is not None and _status_of(latest, now) != "active":
                stale.append({
                    **entry,
                    "ended_lease": latest_view,
                    "ended_reason": _status_of(latest, now),
                    "message": (f"虚拟机 {vm} 仍占用地址 {addr}，但该地址的最新租约"
                                f"（{latest['lease_id']}）已{_status_of(latest, now)}，"
                                "在新快照确认空闲前不得再次分配"),
                })
            elif active is None:
                unexpected.append({
                    **entry,
                    "reason": "no_active_lease",
                    "active_lease": None,
                    "latest_lease": latest_view,
                    "message": f"虚拟机 {vm} 占用地址 {addr}，但台账上该地址无有效租约",
                })
            elif active["vm_id"] != vm:
                unexpected.append({
                    **entry,
                    "reason": "vm_mismatch",
                    "active_lease": _lease_view(active, now),
                    "message": (f"快照显示 {vm} 占用 {addr}，但有效租约归属 {active['vm_id']}"
                                f"（租约 {active['lease_id']}）"),
                })
            elif not is_duplicate:
                consistent.append({
                    **entry,
                    "lease_id": active["lease_id"],
                    "expires_at": _iso(active["expires_at"]),
                })

    # 反向核对：有效租约在租、但未出现在快照中的地址（快照漏报）
    active_rows = conn.execute(
        "SELECT * FROM leases WHERE released_at IS NULL AND expires_at > ?"
        " ORDER BY issued_at DESC, lease_id DESC", (now,)).fetchall()
    # 同一地址理论上至多一条有效租约（分配互斥），这里仍按地址收敛
    active_by_addr = {r["address"]: r for r in reversed(active_rows)}
    missing = []
    for addr, r in active_by_addr.items():
        if addr not in occupiers:
            missing.append({
                "address": addr,
                "vm_id": r["vm_id"],
                "lease_id": r["lease_id"],
                "expires_at": _iso(r["expires_at"]),
                "message": f"有效租约显示 {r['vm_id']} 持有 {addr}，但最新快照未报告该占用",
            })

    return {
        "snapshot_seq": seq,
        "committed_at": _iso(meta["committed_at"]),
        "summary": {
            "entries": len(rows),
            "distinct_addresses": len(occupiers),
            "stale_lease_occupancy": len(stale),
            "duplicate_occupancy": len(duplicate),
            "unexpected_occupancy": len(unexpected),
            "consistent": len(consistent),
            "missing_from_snapshot": len(missing),
        },
        "stale_lease_occupancy": stale,
        "duplicate_occupancy": duplicate,
        "unexpected_occupancy": unexpected,
        "consistent": consistent,
        "missing_from_snapshot": missing,
    }


# ---------------- 租约操作 ----------------

@app.post("/api/leases", tags=["租约"], summary="申请地址（幂等）", status_code=201)
def apply_lease(body: ApplyIn):
    """申请一个地址。同一申请号重试：返回原租约当前状态（200, replayed=true），
    即使原租约已过期也不会再分配新地址。"""
    now = time.time()
    with write_tx() as conn:
        row = conn.execute("SELECT * FROM leases WHERE request_id = ?", (body.request_id,)).fetchone()
        if row is not None:
            if row["vm_id"] != body.vm_id:
                _log_request(conn, body, "rejected_request_id_conflict", row["lease_id"],
                             "申请号已被其它虚拟机占用")
                outcome: tuple[str, Any] = ("error", _conflict(
                    "request_id_conflict", "申请号已被其它虚拟机占用", lease=_lease_view(row, now)))
            else:
                _log_request(conn, body, "replayed", row["lease_id"], "同号重试，返回原租约当前状态")
                outcome = ("ok", JSONResponse(status_code=200, content={
                    "replayed": True,
                    "message": "相同申请号已存在，返回原租约当前状态（不重复分配；过期后也不会借旧号再领地址）",
                    "lease": _lease_view(row, now),
                }))
        else:
            held = conn.execute(
                "SELECT * FROM leases WHERE vm_id = ? AND released_at IS NULL AND expires_at > ?",
                (body.vm_id, now)).fetchone()
            if held is not None:
                _log_request(conn, body, "rejected_vm_already_leased", held["lease_id"],
                             "虚拟机已持有有效地址")
                outcome = ("error", _conflict(
                    "vm_already_leased", "同一虚拟机只能持有一个有效地址",
                    existing_lease=_lease_view(held, now)))
            else:
                busy = {r["address"] for r in conn.execute(
                    "SELECT address FROM leases WHERE released_at IS NULL AND expires_at > ?",
                    (now,)).fetchall()}
                # 快照即事实：最新已提交快照中仍显示占用的地址（含已过期/已释放但未确认空闲的）
                # 一律不得再次分配，直到更新的快照确认其空闲。
                latest_seq = conn.execute("SELECT MAX(seq) FROM snapshots").fetchone()[0]
                snapshot_held: set[str] = set()
                if latest_seq is not None:
                    snapshot_held = {r["address"] for r in conn.execute(
                        "SELECT DISTINCT address FROM snapshot_entries WHERE seq = ?",
                        (latest_seq,)).fetchall()}
                blocked = busy | snapshot_held
                free = [a for a in POOL_ADDRESSES if a not in blocked]
                if not free:
                    _log_request(conn, body, "rejected_pool_exhausted", None, "地址不足")
                    outcome = ("error", _conflict(
                        "pool_exhausted",
                        "地址不足：地址池已耗尽（含最新快照仍显示占用、尚未确认空闲的地址），"
                        "请稍后重试或提交新快照释放占用"))
                else:
                    address = free[0]
                    lease_id = uuid.uuid4().hex
                    token = secrets.token_hex(16)
                    conn.execute(
                        "INSERT INTO leases (lease_id, request_id, vm_id, address, token,"
                        " issued_at, expires_at, node) VALUES (?,?,?,?,?,?,?,?)",
                        (lease_id, body.request_id, body.vm_id, address, token,
                         now, now + body.ttl_seconds, INSTANCE_ID))
                    _log_request(conn, body, "leased", lease_id, f"分配地址 {address}")
                    row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
                    outcome = ("ok", JSONResponse(status_code=201, content={
                        "replayed": False,
                        "message": "租约创建成功",
                        "lease": _lease_view(row, now),
                    }))
    kind, payload = outcome
    if kind == "error":
        raise payload
    return payload


@app.post("/api/leases/{lease_id}/renew", tags=["租约"], summary="续租（需令牌）")
def renew_lease(lease_id: str, body: RenewIn):
    """从当前时刻起延长 ttl_seconds 秒。令牌不匹配、已释放、已过期的租约一律拒绝，
    且不影响地址的当前归属。"""
    now = time.time()
    with write_tx() as conn:
        row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
        if row is None:
            outcome = ("error", _error(404, "lease_not_found", "租约不存在"))
        elif row["token"] != body.token:
            outcome = ("error", _conflict(
                "token_mismatch",
                "令牌不匹配：该租约可能已过期并易主，迟到的续租已被拒绝，不影响当前归属",
                lease=_lease_view(row, now)))
        elif row["released_at"] is not None:
            outcome = ("error", _conflict("lease_released", "租约已释放，无法续租",
                                          lease=_lease_view(row, now)))
        elif row["expires_at"] <= now:
            outcome = ("error", _conflict(
                "lease_expired", "租约已过期，无法续租；过期地址可能已分配给其它虚拟机",
                lease=_lease_view(row, now)))
        else:
            conn.execute("UPDATE leases SET expires_at = ? WHERE lease_id = ?",
                         (now + body.ttl_seconds, lease_id))
            row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
            outcome = ("ok", {"message": "续租成功", "lease": _lease_view(row, now)})
    kind, payload = outcome
    if kind == "error":
        raise payload
    return payload


@app.post("/api/leases/{lease_id}/release", tags=["租约"], summary="释放（需令牌，幂等）")
def release_lease(lease_id: str, body: ReleaseIn):
    """释放租约。重复释放幂等返回；旧令牌迟到的释放只作用于已终止的旧租约，不影响新归属。"""
    now = time.time()
    with write_tx() as conn:
        row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
        if row is None:
            outcome = ("error", _error(404, "lease_not_found", "租约不存在"))
        elif row["token"] != body.token:
            outcome = ("error", _conflict(
                "token_mismatch",
                "令牌不匹配：该租约可能已过期并易主，迟到的释放已被拒绝，不影响当前归属",
                lease=_lease_view(row, now)))
        elif row["released_at"] is not None:
            outcome = ("ok", {"message": "租约此前已释放，重复释放幂等返回（无副作用）",
                              "lease": _lease_view(row, now)})
        else:
            conn.execute("UPDATE leases SET released_at = ? WHERE lease_id = ?", (now, lease_id))
            row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
            outcome = ("ok", {"message": "释放成功", "lease": _lease_view(row, now)})
    kind, payload = outcome
    if kind == "error":
        raise payload
    return payload


# ---------------- 快照对账 API ----------------

@app.post("/api/snapshots", tags=["快照对账"], summary="提交网卡占用快照并返回对账结果",
          status_code=201)
def submit_snapshot(body: SnapshotIn):
    """提交整份网卡占用快照（递增序号 + 全量占用列表），返回与当前租约台账的对账结果。

    - 序号须严格递增：迟到的旧序号返回 409 `snapshot_seq_stale`，不改动现有快照；
    - 同序号同内容重试：幂等返回 200（`replayed=true`），不重复写入、不推进序号；
    - 同序号不同内容：返回 409 `snapshot_seq_conflict`，拒绝更新；
    - 新序号在单个 BEGIN IMMEDIATE 事务内整份写入，任何失败都回滚，上一份完整快照原样保留；
    - 对账只读租约台账，不会改写任何租约。
    """
    now = time.time()
    pairs = _canonical_entries(body.entries)
    digest = _content_hash(pairs)
    with write_tx() as conn:
        meta = _latest_snapshot_meta(conn)
        current_seq = meta["seq"] if meta is not None else None
        if current_seq is not None and body.seq < current_seq:
            outcome: tuple[str, Any] = ("error", _conflict(
                "snapshot_seq_stale",
                f"快照序号 {body.seq} 已过期：当前已提交序号为 {current_seq}，迟到快照已拒绝",
                current_seq=current_seq, submitted_seq=body.seq))
        elif current_seq is not None and body.seq == current_seq:
            if meta["content_hash"] != digest:
                outcome = ("error", _conflict(
                    "snapshot_seq_conflict",
                    f"序号 {body.seq} 已存在但占用列表内容不同：冲突提交已拒绝，"
                    "原快照保持不变（同序号仅允许相同内容的幂等重试）",
                    current_seq=current_seq))
            else:
                rows = conn.execute(
                    "SELECT vm_id, address FROM snapshot_entries WHERE seq = ? ORDER BY ord",
                    (current_seq,)).fetchall()
                report = _reconcile(conn, now)
                outcome = ("ok", JSONResponse(status_code=200, content={
                    "replayed": True,
                    "message": "相同序号与相同内容的快照已提交过，幂等返回（不重复写入）",
                    "snapshot_seq": current_seq,
                    "committed_at": _iso(meta["committed_at"]),
                    "entries": [{"vm_id": r["vm_id"], "address": r["address"]} for r in rows],
                    "reconciliation": report,
                }))
        else:
            conn.execute(
                "INSERT INTO snapshots (seq, content_hash, entry_count, committed_at, committed_node)"
                " VALUES (?,?,?,?,?)",
                (body.seq, digest, len(pairs), now, INSTANCE_ID))
            conn.executemany(
                "INSERT INTO snapshot_entries (seq, vm_id, address, ord) VALUES (?,?,?,?)",
                [(body.seq, vm, addr, i) for i, (vm, addr) in enumerate(pairs)])
            report = _reconcile(conn, now)
            outcome = ("ok", JSONResponse(status_code=201, content={
                "replayed": False,
                "message": f"快照 #{body.seq} 提交成功（{len(pairs)} 条占用），对账结果如下",
                "snapshot_seq": body.seq,
                "committed_at": _iso(now),
                "entries": [{"vm_id": vm, "address": addr} for vm, addr in pairs],
                "reconciliation": report,
            }))
    kind, payload = outcome
    if kind == "error":
        raise payload
    return payload


@app.get("/api/snapshots/latest", tags=["快照对账"], summary="最新已提交快照及其对账结果")
def get_latest_snapshot():
    """返回最后成功提交的快照（重启后仍可读取），并基于当前台账重新计算对账结果。"""
    now = time.time()
    with read_conn() as conn:
        meta = _latest_snapshot_meta(conn)
        if meta is None:
            raise _error(404, "snapshot_not_found", "尚未提交任何网卡占用快照")
        rows = conn.execute(
            "SELECT vm_id, address FROM snapshot_entries WHERE seq = ? ORDER BY ord",
            (meta["seq"],)).fetchall()
        report = _reconcile(conn, now)
    return {
        "snapshot_seq": meta["seq"],
        "content_hash": meta["content_hash"],
        "entry_count": meta["entry_count"],
        "committed_at": _iso(meta["committed_at"]),
        "committed_node": meta["committed_node"],
        "entries": [{"vm_id": r["vm_id"], "address": r["address"]} for r in rows],
        "reconciliation": report,
    }


@app.get("/api/snapshots/reconcile", tags=["快照对账"], summary="仅对账：最新快照 vs 当前租约台账")
def get_reconciliation():
    """不提交快照，只输出最新已提交快照与当前租约台账的对账结果（台账实时状态，含懒过期）。"""
    with read_conn() as conn:
        if _latest_snapshot_meta(conn) is None:
            raise _error(404, "snapshot_not_found", "尚未提交任何网卡占用快照")
        return _reconcile(conn)


@app.get("/api/snapshots/{seq}", tags=["快照对账"], summary="按序号查询历史快照（审计）")
def get_snapshot(seq: int):
    with read_conn() as conn:
        meta = conn.execute("SELECT * FROM snapshots WHERE seq = ?", (seq,)).fetchone()
        if meta is None:
            latest = _latest_snapshot_meta(conn)
            raise _error(404, "snapshot_not_found",
                         f"快照 #{seq} 不存在",
                         current_seq=latest["seq"] if latest is not None else None)
        rows = conn.execute(
            "SELECT vm_id, address FROM snapshot_entries WHERE seq = ? ORDER BY ord",
            (seq,)).fetchall()
    return {
        "snapshot_seq": meta["seq"],
        "content_hash": meta["content_hash"],
        "entry_count": meta["entry_count"],
        "committed_at": _iso(meta["committed_at"]),
        "committed_node": meta["committed_node"],
        "entries": [{"vm_id": r["vm_id"], "address": r["address"]} for r in rows],
    }


@app.get("/api/snapshots", tags=["快照对账"], summary="已提交快照序号列表（审计）")
def list_snapshots(limit: int = Query(default=50, ge=1, le=500)):
    with read_conn() as conn:
        rows = conn.execute(
            "SELECT seq, content_hash, entry_count, committed_at, committed_node"
            " FROM snapshots ORDER BY seq DESC LIMIT ?", (limit,)).fetchall()
    return {
        "count": len(rows),
        "snapshots": [{
            "seq": r["seq"],
            "content_hash": r["content_hash"],
            "entry_count": r["entry_count"],
            "committed_at": _iso(r["committed_at"]),
            "committed_node": r["committed_node"],
        } for r in rows],
    }


# ---------------- 查询 ----------------

@app.get("/api/leases/{lease_id}", tags=["查询"], summary="按租约 ID 查询状态")
def get_lease(lease_id: str):
    with read_conn() as conn:
        row = conn.execute("SELECT * FROM leases WHERE lease_id = ?", (lease_id,)).fetchone()
    if row is None:
        raise _error(404, "lease_not_found", "租约不存在")
    return {"lease": _lease_view(row)}


@app.get("/api/requests/{request_id}", tags=["查询"], summary="按申请号查询租约与申请记录")
def get_request(request_id: str):
    with read_conn() as conn:
        row = conn.execute("SELECT * FROM leases WHERE request_id = ?", (request_id,)).fetchone()
        logs = conn.execute("SELECT * FROM request_log WHERE request_id = ? ORDER BY id",
                            (request_id,)).fetchall()
    if row is None and not logs:
        raise _error(404, "request_not_found", "申请号不存在")
    return {
        "request_id": request_id,
        "lease": _lease_view(row) if row is not None else None,
        "attempts": [
            {
                "seq": log["id"],
                "vm_id": log["vm_id"],
                "result": log["result"],
                "lease_id": log["lease_id"],
                "detail": log["detail"],
                "node": log["node"],
                "at": _iso(log["created_at"]),
            }
            for log in logs
        ],
    }


@app.get("/api/addresses/{address}", tags=["查询"], summary="查询地址当前归属与历史")
def get_address(address: str):
    if address not in POOL_ADDRESSES:
        raise _error(404, "address_not_in_pool", "该地址不在地址池中", pool_cidr=POOL_CIDR)
    now = time.time()
    with read_conn() as conn:
        current = conn.execute(
            "SELECT * FROM leases WHERE address = ? AND released_at IS NULL AND expires_at > ?"
            " ORDER BY issued_at DESC LIMIT 1", (address, now)).fetchone()
        history = conn.execute(
            "SELECT * FROM leases WHERE address = ? ORDER BY issued_at DESC LIMIT 20",
            (address,)).fetchall()
    return {
        "address": address,
        "pool_cidr": POOL_CIDR,
        "state": "leased" if current is not None else "free",
        "current_holder": _lease_view(current, now) if current is not None else None,
        "history": [_lease_view(r, now) for r in history],
    }


@app.get("/api/leases", tags=["查询"], summary="租约台账（可按虚拟机/状态过滤）")
def list_leases(
    vm_id: Optional[str] = Query(default=None, description="按虚拟机 ID 过滤"),
    lease_status: Optional[str] = Query(default=None, alias="status",
                                        pattern="^(active|expired|released)$",
                                        description="按状态过滤：active/expired/released"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    with read_conn() as conn:
        rows = conn.execute("SELECT * FROM leases ORDER BY issued_at DESC, lease_id DESC LIMIT ?",
                            (limit,)).fetchall()
    now = time.time()
    views = [_lease_view(r, now) for r in rows]
    if vm_id is not None:
        views = [v for v in views if v["vm_id"] == vm_id]
    if lease_status is not None:
        views = [v for v in views if v["status"] == lease_status]
    return {"count": len(views), "leases": views}


@app.get("/api/pool", tags=["查询"], summary="地址池总览")
def pool_status():
    now = time.time()
    with read_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM leases WHERE released_at IS NULL AND expires_at > ?", (now,)).fetchall()
        latest_seq = conn.execute("SELECT MAX(seq) FROM snapshots").fetchone()[0]
        snap_rows = []
        if latest_seq is not None:
            snap_rows = conn.execute(
                "SELECT address, vm_id FROM snapshot_entries WHERE seq = ?", (latest_seq,)).fetchall()
    active = {r["address"]: r for r in rows}
    # 同一地址可能被多机占用，快照视角保留全部占用虚拟机
    snapshot_map: dict[str, list[str]] = {}
    for r in snap_rows:
        snapshot_map.setdefault(r["address"], []).append(r["vm_id"])
    addresses = []
    for addr in POOL_ADDRESSES:
        r = active.get(addr)
        snap_vms = snapshot_map.get(addr, [])
        if r is not None:
            state = "leased"
        elif snap_vms:
            # 无有效租约但最新快照仍显示占用：不可再分配，直到新快照确认空闲
            state = "snapshot-held"
        else:
            state = "free"
        addresses.append({
            "address": addr,
            "state": state,
            "vm_id": r["vm_id"] if r is not None else None,
            "lease_id": r["lease_id"] if r is not None else None,
            "expires_at": _iso(r["expires_at"]) if r is not None else None,
            "snapshot_occupants": snap_vms,
            "snapshot_seq": latest_seq if snap_vms else None,
        })
    leased = sum(1 for a in addresses if a["state"] == "leased")
    held = sum(1 for a in addresses if a["state"] == "snapshot-held")
    return {
        "pool_cidr": POOL_CIDR,
        "total": len(addresses),
        "leased": leased,
        "snapshot_held": held,
        "free": len(addresses) - leased - held,
        "latest_snapshot_seq": latest_seq,
        "addresses": addresses,
    }


# ---------------- 系统 ----------------

@app.get("/api/health", tags=["系统"], summary="健康检查")
def health():
    with read_conn() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok", "node": INSTANCE_ID, "time": _iso(time.time())}


@app.get("/", tags=["系统"], summary="服务信息")
def root():
    return {
        "service": "机房虚拟机地址租约服务",
        "node": INSTANCE_ID,
        "docs": "/docs",
        "pool_cidr": POOL_CIDR,
        "pool_size": len(POOL_ADDRESSES),
        "ttl_range_seconds": [MIN_TTL, MAX_TTL],
    }


@app.post("/api/admin/reset", tags=["系统"], summary="清空全部数据（仅自测，需 ALLOW_RESET=1）")
def admin_reset():
    if not ALLOW_RESET:
        raise _error(403, "reset_disabled", "未启用重置功能（设 ALLOW_RESET=1 开启，仅供自测）")
    with write_tx() as conn:
        conn.execute("DELETE FROM leases")
        conn.execute("DELETE FROM request_log")
        conn.execute("DELETE FROM snapshot_entries")
        conn.execute("DELETE FROM snapshots")
    return {"message": "已清空全部租约、申请记录与网卡占用快照", "node": INSTANCE_ID}
