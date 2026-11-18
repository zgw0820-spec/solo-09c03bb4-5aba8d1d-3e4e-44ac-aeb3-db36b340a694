# 机房虚拟机地址租约服务

机房虚拟机启动器从共享的内网 IPv4 地址池领取地址。本服务提供租约 API：
**FastAPI + SQLite（WAL）**，Docker Compose 启动 **两个实例**，挂载同一命名卷
**共享持久数据**（同一个 SQLite 数据库文件）。Swagger UI 可直接完成申请、续租、
释放、归属查询与**网卡占用快照对账**。

## 快速开始

```bash
docker compose up --build
# node1: http://127.0.0.1:8001/docs
# node2: http://127.0.0.1:8002/docs   （两个实例共享同一份租约与快照数据）
```

无 Docker 的本地等价验证（两个进程共享同一个 SQLite 文件）：

```bash
python3 -m pip install -r app/requirements.txt
bash scripts/dev_run_local.sh        # 前台启动 8001/8002 两个实例，Ctrl-C 停止
```

地址池由 `POOL_CIDR` 配置，默认 `10.99.0.0/29`（6 个可用地址，便于演示池满与争抢）。

## API 一览（Swagger: /docs）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/leases` | 申请地址。提交 `vm_id`、`request_id`（申请号）、`ttl_seconds`（1~60 秒）。幂等：同申请号重试返回原租约当前状态（200, `replayed=true`），过期后也不会借旧号再领地址 |
| POST | `/api/leases/{lease_id}/renew` | 续租（需令牌，从当前时刻延长 1~60 秒） |
| POST | `/api/leases/{lease_id}/release` | 释放（需令牌，重复释放幂等） |
| GET | `/api/leases/{lease_id}` | 按租约 ID 查状态（active/expired/released） |
| GET | `/api/requests/{request_id}` | 按申请号查租约与全部申请记录（审计） |
| GET | `/api/addresses/{address}` | 查询地址当前归属与历史 |
| GET | `/api/leases?vm_id=&status=` | 租约台账 |
| GET | `/api/pool` | 地址池总览（含 `snapshot-held` 状态） |
| POST | `/api/snapshots` | **提交网卡占用快照并返回对账结果**（递增 `seq` + 全量 `entries`） |
| GET | `/api/snapshots/latest` | 最新已提交快照 + 基于当前台账的对账结果 |
| GET | `/api/snapshots/reconcile` | 仅对账：最新快照 vs 当前租约台账 |
| GET | `/api/snapshots/{seq}` | 按序号查历史快照（审计） |
| GET | `/api/snapshots` | 已提交快照序号列表 |
| GET | `/api/health` | 健康检查 |

错误统一为 `{"detail": {"error": <代码>, "message": <中文说明>, ...}}`：
`pool_exhausted`（地址不足）、`vm_already_leased`（一机一址）、
`token_mismatch`（令牌不符）、`lease_expired` / `lease_released`、`request_id_conflict`、
`snapshot_seq_stale`（迟到旧序号）、`snapshot_seq_conflict`（同序号内容冲突）、
`snapshot_not_found`。

## 网卡占用快照对账

机房运维拿到虚拟化平台的整份网卡占用快照（虚拟机 ID + 池内 IPv4），用
`POST /api/snapshots` 提交 `{seq, entries}`，服务在同一响应中返回与租约台账的对账结果：

- **`stale_lease_occupancy`**：虚拟机仍占用某地址，但该地址的最新租约已结束
  （`expired` / `released`）——即仍占用已结束租约的地址；
- **`duplicate_occupancy`**：同一地址在快照中被多台虚拟机占用；
- **`unexpected_occupancy`**：快照占用与有效租约不符，细分两种 `reason`：
  `no_active_lease`（台账无有效租约）与 `vm_mismatch`（有效租约归属另一台虚拟机）；
- **`missing_from_snapshot`**：反向核对——有效租约在租、但快照未报告该占用（快照漏报）；
- **`consistent`**：占用与有效租约完全一致的条目。

序号与幂等规则：

- `seq` 必须为正整数且**严格递增**；迟到的旧序号返回 409 `snapshot_seq_stale`，
  不改动任何现有数据；
- **同序号同内容**重试：幂等返回 200（`replayed=true`），不重复写入、不推进序号；
- **同序号不同内容**：返回 409 `snapshot_seq_conflict`，拒绝更新，原快照完整保留；
- 池外地址在参数校验层直接 422 拒绝，不进入数据库事务。

快照即事实（占用阻断再分配）：

- 过期或已释放的地址，只要仍出现在**最新已提交快照**中，就不会被再次分配
  （`/api/pool` 标记为 `snapshot-held`），直到更新的快照确认其空闲；
- 新快照从占用列表移除该地址后，地址立即恢复可分配；
- 快照与申请都走 `BEGIN IMMEDIATE` 写事务，在 SQLite 层全局串行化：
  快照提交与两实例的申请交错时，分配决策与对账永远依据同一份已提交状态；
- 快照整份写入在单个事务内完成，任何失败都回滚，**上一份完整快照原样保留**；
- 快照序号与内容持久化在共享 SQLite 中，实例重启后继续使用最后成功提交的序号。

对账是**只读**操作：只读取租约台账进行比对，不会创建、修改或释放任何租约。

## 设计要点

- **分配原子性**：所有写操作走 `BEGIN IMMEDIATE` 事务。两个实例共用同一数据库文件，
  写事务在 SQLite 层串行化，“查空闲地址 + 写租约”是原子操作——两个实例争抢最后一个
  地址时只有一方成功，另一方收到 `pool_exhausted`（地址不足）。
- **幂等申请**：`request_id` 有唯一约束，一个申请号终身只对应一条租约。同号重试
  （包括打到另一个实例）返回原租约的当前状态；原租约过期后重试只会看到 `expired`，
  不会借旧号再领地址。
- **一机一址**：申请时在事务内检查该虚拟机是否已持有有效租约，已持有则拒绝并返回现有租约。
- **懒过期**：`expires_at <= now` 即视为过期，地址立即可回收，无需后台清理线程；
  状态（active/expired/released）由字段实时推导。
- **令牌防串**：续租/释放必须携带申请时返回的随机令牌。地址过期易主后，旧令牌迟到的
  续租/释放只命中已终止的旧租约行，不影响新归属。
- **持久化与审计**：`leases`（租约全量历史）、`request_log`（每次申请的受理结果）、
  `snapshots` / `snapshot_entries`（全部历史快照）都落在共享卷的 SQLite 文件中，
  实例重启后仍可核对。

## 自测

```bash
python3 scripts/selftest.py           # 端到端：两实例需已启动（dev_run_local.sh）
python3 scripts/selftest_restart.py   # 重启持久化：自起自停独立实例，无需预先启动
```

`selftest.py` 覆盖：参数校验（1~60 秒）、基本申请、跨实例同号重试、一机一址、
池满“地址不足”、续租（含错误令牌）、释放幂等、过期交错（旧号重试 / 旧令牌迟到 /
地址易主）、双实例争抢最后一个地址（2 抢 1）、满池争抢（12 抢 6）、申请记录审计；
以及快照专项 S1~S6：对账三类差异、同序号幂等/冲突/旧序号拒绝、池外地址拒绝、
快照占用阻断再分配（含过期未确认空闲）、新快照确认空闲后恢复分配、
快照提交与两实例申请并发交错的一致性不变量。

`selftest_restart.py` 覆盖：快照序号/内容/哈希与租约一并持久化、杀进程重启后
继续使用最后成功提交的序号、旧序号在重启后仍被拒绝、提交失败时上一份快照保留。

## 已知限制

- 为便于演示，租约视图（含令牌）对查询接口可见；生产环境应对 GET 接口隐藏令牌并加访问控制。
- `ALLOW_RESET=1` 仅在自测时开启（compose 中默认开启，生产部署请移除）。
- 两个实例须运行在同一台宿主机共享卷上（SQLite 文件锁依赖本地文件系统；
  不支持 NFS 等网络文件系统上的多机共享）。
