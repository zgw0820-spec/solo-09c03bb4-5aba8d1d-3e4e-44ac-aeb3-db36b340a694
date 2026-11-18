#!/usr/bin/env python3
"""租约服务端到端自测（仅依赖标准库）。

覆盖：申请 / 同号幂等重试（跨实例）/ 一机一址 / 池满“地址不足” / 续租（含错误令牌）/
释放（幂等）/ 过期后旧号重试与旧令牌迟到操作 / 地址回收再分配 /
双实例争抢最后一个地址 / 双实例满池争抢 / 申请记录审计 /
网卡占用快照对账（幂等/冲突/旧序号、占用阻断再分配、对账三类差异、快照与申请交错一致性）。

前提：两个实例已启动且共享同一数据库，地址池为 10.99.0.0/29（6 个地址），ALLOW_RESET=1。
用法：python3 scripts/selftest.py   （可用 BASE1/BASE2 环境变量覆盖地址）
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE1 = os.environ.get("BASE1", "http://127.0.0.1:8001")
BASE2 = os.environ.get("BASE2", "http://127.0.0.1:8002")

passed = 0
failed = 0


def check(name, cond, extra=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"    PASS  {name}")
    else:
        failed += 1
        print(f"    FAIL  {name}  {extra}")


def req(method, base, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(base + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=15) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {}), resp.headers.get("X-Served-By")
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw}
        return e.code, payload, (e.headers.get("X-Served-By") if e.headers else None)


def apply(base, vm, rq, ttl):
    return req("POST", base, "/api/leases",
               {"vm_id": vm, "request_id": rq, "ttl_seconds": ttl})


def snapshot(base, seq, entries):
    return req("POST", base, "/api/snapshots", {"seq": seq, "entries": entries})


def reconcile(base):
    return req("GET", base, "/api/snapshots/reconcile")[1]


def pool(base):
    return req("GET", base, "/api/pool")[1]


def main():
    print(f"== 目标实例: {BASE1} / {BASE2}\n")

    print("-- P0 健康检查 / 参数校验 / 重置")
    s, b, _ = req("GET", BASE1, "/api/health")
    check("node1 健康", s == 200 and b.get("node") == "node1", f"{s} {b}")
    s, b, _ = req("GET", BASE2, "/api/health")
    check("node2 健康", s == 200 and b.get("node") == "node2", f"{s} {b}")
    s, b, _ = apply(BASE1, "vm-bad", "req-bad-0", 0)
    check("租期 0 秒被拒绝(422)", s == 422, f"{s} {b}")
    s, b, _ = apply(BASE1, "vm-bad", "req-bad-61", 61)
    check("租期 61 秒被拒绝(422)", s == 422, f"{s} {b}")
    s, b, _ = req("POST", BASE2, "/api/admin/reset")
    check("通过 node2 重置共享数据", s == 200, f"{s} {b}")
    p = pool(BASE1)
    check("node1 看到池已清空（共享持久数据）", p.get("free") == 6 and p.get("total") == 6,
          json.dumps(p, ensure_ascii=False))

    print("-- P1 基本申请（node1）")
    s, b, n = apply(BASE1, "vm-a", "req-a", 30)
    la = b.get("lease", {})
    check("申请成功(201)", s == 201, f"{s} {b}")
    check("分到池内最低空闲地址 10.99.0.1", la.get("address") == "10.99.0.1", str(la))
    check("响应带租约令牌", bool(la.get("token")), str(la))
    check("X-Served-By 为 node1", n == "node1", str(n))

    print("-- P2 同号重试（打到 node2，跨实例幂等）")
    s, b, n = apply(BASE2, "vm-a", "req-a", 30)
    lb = b.get("lease", {})
    check("重试返回 200 且 replayed=true", s == 200 and b.get("replayed") is True, f"{s} {b}")
    check("返回原租约（lease_id/地址/令牌一致）",
          lb.get("lease_id") == la.get("lease_id") and lb.get("address") == la.get("address")
          and lb.get("token") == la.get("token"), f"{la} vs {lb}")
    check("原租约状态为 active", lb.get("status") == "active", str(lb))
    check("由 node2 处理（共享库可见）", n == "node2", str(n))

    print("-- P3 同一虚拟机只能持有一个有效地址")
    s, b, _ = apply(BASE1, "vm-a", "req-a2", 30)
    check("重复申请被拒(409 vm_already_leased)",
          s == 409 and b.get("detail", {}).get("error") == "vm_already_leased", f"{s} {b}")
    check("错误响应附带已持有租约",
          b.get("detail", {}).get("existing_lease", {}).get("address") == "10.99.0.1", str(b))

    print("-- P4 填满地址池（两实例交替受理）")
    leases = {"vm-a": la}
    ok = True
    for i, vm in enumerate(["vm-b", "vm-c", "vm-d", "vm-e", "vm-f"]):
        base = BASE1 if i % 2 == 0 else BASE2
        ttl = 30 if vm == "vm-b" else 60
        s, b, _ = apply(base, vm, f"req-{vm}", ttl)
        leases[vm] = b.get("lease", {})
        ok = ok and s == 201
    addrs = [leases[v].get("address") for v in ["vm-a", "vm-b", "vm-c", "vm-d", "vm-e", "vm-f"]]
    check("5 台虚拟机全部申请成功", ok, str(addrs))
    check("6 个地址互不重复", len(set(addrs)) == 6, str(addrs))
    p = pool(BASE2)
    check("node2 视角：池已满 free=0", p.get("free") == 0 and p.get("leased") == 6,
          json.dumps(p, ensure_ascii=False))

    print("-- P5 池满返回“地址不足”")
    s, b, _ = apply(BASE1, "vm-g", "req-g", 30)
    check("返回 409 pool_exhausted", s == 409 and b.get("detail", {}).get("error") == "pool_exhausted",
          f"{s} {b}")
    check("报错文案含“地址不足”", "地址不足" in json.dumps(b, ensure_ascii=False), str(b))

    print("-- P6 续租")
    s, b, _ = req("POST", BASE1, f"/api/leases/{leases['vm-b']['lease_id']}/renew",
                  {"token": "wrong-token", "ttl_seconds": 30})
    check("错误令牌续租被拒(409 token_mismatch)",
          s == 409 and b.get("detail", {}).get("error") == "token_mismatch", f"{s} {b}")
    old_exp = leases["vm-b"]["expires_at"]
    s, b, _ = req("POST", BASE2, f"/api/leases/{leases['vm-b']['lease_id']}/renew",
                  {"token": leases["vm-b"]["token"], "ttl_seconds": 60})
    nb = b.get("lease", {})
    check("正确令牌续租成功", s == 200 and nb.get("status") == "active", f"{s} {b}")
    check("过期时间被延长", nb.get("expires_at", "") > old_exp, f"{old_exp} -> {nb.get('expires_at')}")
    leases["vm-b"] = nb

    print("-- P7 释放（幂等）")
    s, b, _ = req("POST", BASE1, f"/api/leases/{leases['vm-c']['lease_id']}/release",
                  {"token": "bad"})
    check("错误令牌释放被拒(409)", s == 409 and b.get("detail", {}).get("error") == "token_mismatch",
          f"{s} {b}")
    s, b, _ = req("POST", BASE2, f"/api/leases/{leases['vm-c']['lease_id']}/release",
                  {"token": leases["vm-c"]["token"]})
    check("释放成功，状态 released", s == 200 and b.get("lease", {}).get("status") == "released",
          f"{s} {b}")
    s, b, _ = req("POST", BASE1, f"/api/leases/{leases['vm-c']['lease_id']}/release",
                  {"token": leases["vm-c"]["token"]})
    check("重复释放幂等返回(200 无副作用)", s == 200, f"{s} {b}")
    check("释放后池有 1 个空闲地址", pool(BASE1).get("free") == 1, json.dumps(pool(BASE1)))

    print("-- P8 释放后的地址可再分配")
    s, b, _ = apply(BASE2, "vm-h", "req-h", 60)
    lh = b.get("lease", {})
    check("vm-h 申请成功", s == 201, f"{s} {b}")
    check("复用了 vm-c 释放的 10.99.0.3", lh.get("address") == "10.99.0.3", str(lh))
    leases["vm-h"] = lh

    print("-- P9 过期交错：旧号重试 / 旧令牌迟到 / 地址易主")
    req("POST", BASE1, f"/api/leases/{leases['vm-d']['lease_id']}/release",
        {"token": leases["vm-d"]["token"]})
    s, b, _ = apply(BASE1, "vm-i", "req-i", 2)
    li = b.get("lease", {})
    check("vm-i 短租约(2s)申请成功，地址 10.99.0.4", s == 201 and li.get("address") == "10.99.0.4",
          f"{s} {b}")
    time.sleep(3)
    s, b, _ = req("GET", BASE2, f"/api/leases/{li['lease_id']}")
    check("过期后状态为 expired", s == 200 and b.get("lease", {}).get("status") == "expired",
          f"{s} {b}")
    free_before = pool(BASE1).get("free")  # 过期已释放 10.99.0.4，取重试前的基准
    s, b, _ = apply(BASE2, "vm-i", "req-i", 30)
    li2 = b.get("lease", {})
    check("过期后同号重试：仍返回原租约(200 replayed)",
          s == 200 and b.get("replayed") is True and li2.get("lease_id") == li.get("lease_id"),
          f"{s} {b}")
    check("原租约当前状态为 expired，未借旧号再领地址",
          li2.get("status") == "expired" and li2.get("address") == "10.99.0.4", str(li2))
    s, b, _ = req("GET", BASE1, "/api/leases?vm_id=vm-i&status=active")
    check("vm-i 名下无有效租约", s == 200 and b.get("count") == 0, f"{s} {b}")
    check("重试未消耗空闲地址", pool(BASE1).get("free") == free_before,
          f"before={free_before} after={pool(BASE1).get('free')}")
    s, b, _ = apply(BASE2, "vm-j", "req-j", 60)
    lj = b.get("lease", {})
    check("过期地址分配给 vm-j（10.99.0.4）", s == 201 and lj.get("address") == "10.99.0.4",
          f"{s} {b}")
    s, b, _ = req("POST", BASE1, f"/api/leases/{li['lease_id']}/renew",
                  {"token": li["token"], "ttl_seconds": 30})
    check("旧令牌迟到的续租被拒(409 lease_expired)",
          s == 409 and b.get("detail", {}).get("error") == "lease_expired", f"{s} {b}")
    s, b, _ = req("POST", BASE2, f"/api/leases/{li['lease_id']}/release", {"token": li["token"]})
    check("旧令牌迟到的释放只终结旧租约(200)", s == 200, f"{s} {b}")
    s, b, _ = req("GET", BASE1, "/api/addresses/10.99.0.4")
    holder = b.get("current_holder") or {}
    check("新归属不受影响：10.99.0.4 仍归 vm-j 且 active",
          holder.get("vm_id") == "vm-j" and holder.get("status") == "active", str(b))

    print("-- P9.5 申请记录审计")
    s, b, _ = req("GET", BASE2, "/api/requests/req-a")
    results = [a.get("result") for a in b.get("attempts", [])]
    check("req-a 申请记录含 leased + replayed", "leased" in results and "replayed" in results,
          str(results))
    nodes = {a.get("node") for a in b.get("attempts", [])}
    check("申请记录可见两个实例的受理痕迹", nodes == {"node1", "node2"}, str(nodes))
    s, b, _ = req("GET", BASE1, "/api/requests/req-g")
    results = [a.get("result") for a in b.get("attempts", [])]
    check("req-g 记录为 rejected_pool_exhausted", results == ["rejected_pool_exhausted"], str(results))

    print("-- P10 双实例争抢最后一个地址")
    req("POST", BASE1, "/api/admin/reset")
    for i in range(1, 6):
        apply(BASE1, f"vm-k{i}", f"req-k{i}", 60)
    check("预备：仅剩 1 个空闲地址", pool(BASE2).get("free") == 1, json.dumps(pool(BASE2)))
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(apply, BASE1, "vm-r1", "req-r1", 60)
        f2 = ex.submit(apply, BASE2, "vm-r2", "req-r2", 60)
        r1, r2 = f1.result(), f2.result()
    codes = sorted([r1[0], r2[0]])
    check("争抢结果恰好一个 201 一个 409", codes == [201, 409], f"{r1[0]} {r2[0]}")
    winner = r1 if r1[0] == 201 else r2
    check("胜者拿到最后的 10.99.0.6", winner[1].get("lease", {}).get("address") == "10.99.0.6",
          str(winner[1]))
    loser = r2 if r1[0] == 201 else r1
    check("败者收到地址不足",
          loser[1].get("detail", {}).get("error") == "pool_exhausted", str(loser[1]))
    check("争抢后池满", pool(BASE1).get("free") == 0, json.dumps(pool(BASE1)))

    print("-- P11 双实例满池争抢（12 并发抢 6 个地址）")
    req("POST", BASE2, "/api/admin/reset")

    def contender(i):
        base = BASE1 if i % 2 == 0 else BASE2
        return apply(base, f"vm-c{i:02d}", f"req-c{i:02d}", 60)

    with ThreadPoolExecutor(max_workers=12) as ex:
        results = list(ex.map(contender, range(1, 13)))
    wins = [r for r in results if r[0] == 201]
    losses = [r for r in results if r[0] == 409]
    check("恰好 6 个成功", len(wins) == 6, f"wins={len(wins)} losses={len(losses)}")
    check("恰好 6 个池满失败", len(losses) == 6 and all(
        r[1].get("detail", {}).get("error") == "pool_exhausted" for r in losses), str(losses[:1]))
    win_addrs = [r[1].get("lease", {}).get("address") for r in wins]
    check("6 个成功地址互不重复", len(set(win_addrs)) == 6, str(win_addrs))
    p = pool(BASE2)
    check("node2 视角：6 租 0 空闲", p.get("leased") == 6 and p.get("free") == 0,
          json.dumps(p, ensure_ascii=False))

    # ================= 网卡占用快照对账 =================

    print("-- S1 重置后提交首份快照（含正常占用 / 无租约占用 / 池外地址）")
    req("POST", BASE1, "/api/admin/reset")
    s, b, _ = apply(BASE1, "vm-a", "req-a", 60)
    la = b["lease"]  # 10.99.0.1
    s, b, _ = snapshot(BASE1, 1, [
        {"vm_id": "vm-a", "address": "10.99.0.1"},   # 与有效租约一致
        {"vm_id": "vm-z", "address": "10.99.0.2"},   # 台账无有效租约
        {"vm_id": "vm-x", "address": "10.99.0.5"},
        {"vm_id": "vm-y", "address": "10.99.0.5"},   # 同一地址多机占用
    ])
    check("快照提交成功(201)", s == 201 and b.get("replayed") is False and b.get("snapshot_seq") == 1,
          f"{s} {b}")
    rep = b.get("reconciliation", {})
    check("对账：1 条一致占用", rep.get("summary", {}).get("consistent") == 1, json.dumps(rep, ensure_ascii=False))
    check("对账：1 个地址多机占用(10.99.0.5)",
          [d.get("address") for d in rep.get("duplicate_occupancy", [])] == ["10.99.0.5"]
          and rep["duplicate_occupancy"][0]["occupying_vms"] == ["vm-x", "vm-y"],
          json.dumps(rep.get("duplicate_occupancy"), ensure_ascii=False))
    unexp = {(u["address"], u["vm_id"], u["reason"]) for u in rep.get("unexpected_occupancy", [])}
    check("对账：无有效租约的占用被标 unexpected(no_active_lease)",
          ("10.99.0.2", "vm-z", "no_active_lease") in unexp, json.dumps(rep.get("unexpected_occupancy"), ensure_ascii=False))
    check("对账：多机占用同时按各占用方记入 unexpected",
          ("10.99.0.5", "vm-x", "no_active_lease") in unexp
          and ("10.99.0.5", "vm-y", "no_active_lease") in unexp, str(unexp))
    s, b, _ = snapshot(BASE1, 1, [{"vm_id": "vm-bad", "address": "10.0.0.99"}])
    check("池外地址被拒绝(422)，快照不写入", s == 422, f"{s} {b}")
    s, b, _ = req("GET", BASE2, "/api/snapshots/latest")
    check("node2 读到 node1 提交的快照 #1（共享库）",
          s == 200 and b.get("snapshot_seq") == 1 and len(b.get("entries", [])) == 4, f"{s} {b}")

    print("-- S2 同序号幂等 / 冲突 / 迟到旧序号")
    s, b, _ = snapshot(BASE2, 1, [
        {"vm_id": "vm-a", "address": "10.99.0.1"},
        {"vm_id": "vm-z", "address": "10.99.0.2"},
        {"vm_id": "vm-x", "address": "10.99.0.5"},
        {"vm_id": "vm-y", "address": "10.99.0.5"},
    ])
    check("同序号同内容重试：200 replayed 不重复写入",
          s == 200 and b.get("replayed") is True, f"{s} {b}")
    snaps = req("GET", BASE1, "/api/snapshots")[1]
    check("快照列表仍只有 1 份", snaps.get("count") == 1, json.dumps(snaps))
    s, b, _ = snapshot(BASE1, 1, [{"vm_id": "vm-a", "address": "10.99.0.1"}])
    check("同序号不同内容：409 snapshot_seq_conflict",
          s == 409 and b.get("detail", {}).get("error") == "snapshot_seq_conflict", f"{s} {b}")
    s, b, _ = req("GET", BASE2, "/api/snapshots/latest")
    check("冲突后原快照完整保留（仍为 4 条）",
          b.get("snapshot_seq") == 1 and len(b.get("entries", [])) == 4, f"{s} {len(b.get('entries', []))}")
    s, b, _ = snapshot(BASE1, 0, [])
    check("序号 0 被参数校验拒绝(422)", s == 422, f"{s} {b}")

    print("-- S3 快照占用阻断再分配（过期/释放地址未确认空闲前不得分配）")
    # vm-z 占用的 10.99.0.2 从无租约；vm-a 释放 10.99.0.1 后它仍在快照中
    s, b, _ = req("POST", BASE2, f"/api/leases/{la['lease_id']}/release", {"token": la["token"]})
    check("释放 vm-a 的租约", s == 200 and b["lease"]["status"] == "released", f"{s} {b}")
    p = pool(BASE1)
    held = {a["address"]: a["state"] for a in p["addresses"]}
    check("池总览：10.99.0.1/2/5 为 snapshot-held，其余 free",
          held.get("10.99.0.1") == "snapshot-held" and held.get("10.99.0.2") == "snapshot-held"
          and held.get("10.99.0.5") == "snapshot-held" and held.get("10.99.0.3") == "free"
          and p.get("free") == 3 and p.get("snapshot_held") == 3, json.dumps(p, ensure_ascii=False))
    s, b, _ = apply(BASE2, "vm-new1", "req-new1", 60)
    check("新申请跳过快照占用地址，分到首个确认空闲的 10.99.0.3",
          s == 201 and b["lease"]["address"] == "10.99.0.3", f"{s} {b}")
    s, b, _ = apply(BASE1, "vm-new2", "req-new2", 60)
    check("继续分配 10.99.0.4", s == 201 and b["lease"]["address"] == "10.99.0.4", f"{s} {b}")
    s, b, _ = apply(BASE2, "vm-new3", "req-new3", 60)
    check("再分配 10.99.0.6", s == 201 and b["lease"]["address"] == "10.99.0.6", f"{s} {b}")
    s, b, _ = apply(BASE1, "vm-new4", "req-new4", 60)
    check("三个确认空闲地址用完后：409 pool_exhausted（快照占用未释放）",
          s == 409 and b["detail"]["error"] == "pool_exhausted", f"{s} {b}")

    print("-- S4 对账：释放后仍占用 -> stale_lease_occupancy；归属不符 -> vm_mismatch")
    rep = reconcile(BASE1)
    stale = {(d["address"], d["vm_id"], d["ended_reason"]) for d in rep.get("stale_lease_occupancy", [])}
    check("vm-a 释放后仍在快照占用 10.99.0.1 -> stale(released)",
          ("10.99.0.1", "vm-a", "released") in stale, json.dumps(rep.get("stale_lease_occupancy"), ensure_ascii=False))
    # 新快照：10.99.0.3 实际归 vm-new1，快照却报 vm-wrong；并确认 10.99.0.1 已空闲
    s, b, _ = snapshot(BASE2, 2, [
        {"vm_id": "vm-wrong", "address": "10.99.0.3"},
        {"vm_id": "vm-z", "address": "10.99.0.2"},
    ])
    check("快照 #2 提交（序号推进，迟到的 #1 之后应被拒）",
          s == 201 and b["snapshot_seq"] == 2, f"{s} {b}")
    s, b, _ = snapshot(BASE1, 1, [{"vm_id": "vm-a", "address": "10.99.0.1"}])
    check("迟到旧序号 #1：409 snapshot_seq_stale",
          s == 409 and b["detail"]["error"] == "snapshot_seq_stale"
          and b["detail"].get("current_seq") == 2, f"{s} {b}")
    rep = req("GET", BASE1, "/api/snapshots/reconcile")[1]
    mm = [u for u in rep.get("unexpected_occupancy", []) if u.get("reason") == "vm_mismatch"]
    check("对账：10.99.0.3 归属不符 -> vm_mismatch（有效租约归 vm-new1）",
          len(mm) == 1 and mm[0]["address"] == "10.99.0.3" and mm[0]["vm_id"] == "vm-wrong"
          and mm[0]["active_lease"]["vm_id"] == "vm-new1", json.dumps(mm, ensure_ascii=False))
    check("对账：10.99.0.2 仍为无有效租约占用",
          any(u["address"] == "10.99.0.2" and u["reason"] == "no_active_lease"
              for u in rep["unexpected_occupancy"]), json.dumps(rep["unexpected_occupancy"], ensure_ascii=False))
    check("对账：有效租约 vm-new2/vm-new3 未出现在快照 -> missing_from_snapshot",
          {m["address"] for m in rep["missing_from_snapshot"]} == {"10.99.0.4", "10.99.0.6"},
          json.dumps(rep["missing_from_snapshot"], ensure_ascii=False))
    check("对账不改写台账：vm-new1 的租约仍 active 且归属不变",
          req("GET", BASE2, "/api/addresses/10.99.0.3")[1]["current_holder"]["vm_id"] == "vm-new1", "")

    print("-- S5 新快照确认空闲后地址恢复可分配")
    s, b, _ = snapshot(BASE1, 3, [
        {"vm_id": "vm-z", "address": "10.99.0.2"},  # 仅保留这一处占用
    ])
    check("快照 #3 确认 10.99.0.1/10.99.0.3 空闲", s == 201, f"{s} {b}")
    p = pool(BASE2)
    # 此时 vm-new1/new2/new3 的 3 条有效租约仍在（.3/.4/.6），.2 仍被快照占用，真正空闲为 .1/.5
    check("池总览：leased=3、snapshot_held=1、free=2",
          p.get("leased") == 3 and p.get("snapshot_held") == 1 and p.get("free") == 2,
          json.dumps(p, ensure_ascii=False))
    s, b, _ = apply(BASE2, "vm-reuse", "req-reuse", 60)
    check("恢复后新申请可拿到 10.99.0.1（最低空闲）",
          s == 201 and b["lease"]["address"] == "10.99.0.1", f"{s} {b}")

    print("-- S6 快照与申请交错：分配与对账基于一致的已提交状态（双实例并发）")

    # 场景 A（确定性）：快照先占用 .1，并发申请只可分 .2~.6；
    # 再让“新快照追加占用 .2”与 3 个申请并发，用可串行化不变量核对结果。
    req("POST", BASE1, "/api/admin/reset")
    s, _, _ = snapshot(BASE1, 1, [{"vm_id": "vm-hold", "address": "10.99.0.1"}])
    check("交错 A：预置快照 #1 占用 10.99.0.1", s == 201, f"{s}")
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [
            ex.submit(snapshot, BASE2, 2, [
                {"vm_id": "vm-hold", "address": "10.99.0.1"},
                {"vm_id": "vm-hold2", "address": "10.99.0.2"},
            ]),
            ex.submit(apply, BASE1, "vm-t1", "req-t1", 60),
            ex.submit(apply, BASE2, "vm-t2", "req-t2", 60),
            ex.submit(apply, BASE1, "vm-t3", "req-t3", 60),
        ]
        results = [f.result() for f in futs]
    check("交错 A：快照 #2 提交成功", results[0][0] == 201 and results[0][1]["snapshot_seq"] == 2,
          f"{results[0][0]} {results[0][1]}")
    won = [r for r in results[1:] if r[0] == 201]
    got = sorted(r[1]["lease"]["address"] for r in won)
    check("交错 A：3 个申请全部成功（池内足够）", len(won) == 3, str(got))
    check("交错 A：预置快照 #1 的 10.99.0.1 绝不可能被分走",
          "10.99.0.1" not in got, str(got))
    check("交错 A：分配地址互不重复", len(set(got)) == len(got), str(got))
    # 可串行化不变量：写事务在 SQLite 层串行化，只存在两种合法历史——
    #  (1) 快照 #2 先提交：.2 被阻断，任何租约都拿不到 .2；
    #  (2) 某申请先提交拿到 .2：最终状态与快照 #2 不符，对账必须如实报 vm_mismatch。
    # 两种历史都要求“分配与对账依据同一份已提交状态”，不允许出现分配了却对不出来的情形。
    rep = reconcile(BASE1)
    flagged = {(u["address"], u["reason"]) for u in rep.get("unexpected_occupancy", [])}
    lease_2 = req("GET", BASE1, "/api/addresses/10.99.0.2")[1]
    if lease_2["state"] == "leased":
        check("交错 A：.2 已出租（申请事务先于快照提交）-> 对账必须报 vm_mismatch",
              ("10.99.0.2", "vm_mismatch") in flagged
              and next(u for u in rep["unexpected_occupancy"] if u["address"] == "10.99.0.2"
                       )["active_lease"]["lease_id"] == lease_2["current_holder"]["lease_id"],
              json.dumps(rep.get("unexpected_occupancy"), ensure_ascii=False))
    else:
        check("交错 A：快照 #2 先提交，.2 被阻断无租约", True)

    # 场景 B（确定性）：先提交占满 6 地址的快照，再并发 6 个申请 -> 必然全部 pool_exhausted
    req("POST", BASE2, "/api/admin/reset")
    full = [{"vm_id": f"vm-h{i}", "address": f"10.99.0.{i}"} for i in range(1, 7)]
    s, b, _ = snapshot(BASE1, 1, full)
    check("交错 B：满占用快照 #1 提交成功", s == 201, f"{s} {b}")
    with ThreadPoolExecutor(max_workers=6) as ex:
        app_r = list(ex.map(lambda i: apply(BASE2 if i % 2 else BASE1,
                                            f"vm-b{i}", f"req-b{i}", 60), range(6)))
    check("交错 B：6 个并发申请全部 409 pool_exhausted（快照占满，无地址可分）",
          all(r[0] == 409 and r[1]["detail"]["error"] == "pool_exhausted" for r in app_r),
          str([(r[0], r[1].get("detail", {}).get("error")) for r in app_r]))
    check("交错 B：台账无任何租约（快照不产生租约、不改写台账）",
          req("GET", BASE1, "/api/leases?limit=100")[1]["count"] == 0, "")
    # 新快照确认全部空闲后立即恢复可分配
    s, _, _ = snapshot(BASE2, 2, [])
    check("交错 B：空快照 #2 确认全部空闲", s == 201, f"{s}")
    s, b, _ = apply(BASE1, "vm-back", "req-back", 60)
    check("交错 B：确认空闲后立刻可申请到 10.99.0.1",
          s == 201 and b["lease"]["address"] == "10.99.0.1", f"{s} {b}")

    # 场景 C（不变量压测）：多轮“快照提交 + 并发申请”交错。
    # 每轮结束后，对快照占用地址只允许两种一致状态：
    #  (1) 无有效租约（快照先提交，地址被阻断）；
    #  (2) 有有效租约但归属虚拟机与快照不同（申请事务先提交），此时对账必须报 vm_mismatch。
    req("POST", BASE1, "/api/admin/reset")
    violations = []
    for rnd in range(8):
        base_seq = rnd * 10 + 1
        held_addr = f"10.99.0.{(rnd % 6) + 1}"
        holder_vm = f"vm-h{rnd}"
        with ThreadPoolExecutor(max_workers=4) as ex:
            f_snap = ex.submit(snapshot, BASE1 if rnd % 2 else BASE2, base_seq,
                               [{"vm_id": holder_vm, "address": held_addr}])
            f_apps = [ex.submit(apply, BASE1 if i % 2 else BASE2,
                                f"vm-r{rnd}-{i}", f"req-r{rnd}-{i}", 60) for i in range(3)]
            snap_r = f_snap.result()
            apps = [f.result() for f in f_apps]
        if snap_r[0] != 201:
            violations.append(f"round{rnd}: snapshot failed {snap_r[0]}")
            continue
        # 注意：核对必须在“确认空闲的空快照”之前，基于本轮快照 base_seq 的已提交状态
        rep = reconcile(BASE2)
        mismatches = {(u["address"], u["vm_id"], u["reason"])
                      for u in rep.get("unexpected_occupancy", [])}
        addr_view = req("GET", BASE1, f"/api/addresses/{held_addr}")[1]
        if addr_view["state"] == "free":
            # 状态 (1)：快照先提交，地址被阻断；实现会以 no_active_lease 报告该占用。
            if (held_addr, holder_vm, "no_active_lease") not in mismatches:
                violations.append(f"round{rnd}: {held_addr} blocked but reconciliation missed report")
        else:
            holder = addr_view["current_holder"]
            if holder["vm_id"] == holder_vm:
                violations.append(f"round{rnd}: {held_addr} leased to snapshot VM without lease semantics")
            elif (held_addr, holder_vm, "vm_mismatch") not in mismatches:
                # 状态 (2)：申请先提交，对账必须把快照占用方与实际归属的不符如实报出
                violations.append(f"round{rnd}: {held_addr} leased to {holder['vm_id']} "
                                  "but reconciliation missed vm_mismatch")
        # 下一轮先确认空闲，保证地址可回收、轮次之间互不干扰
        snapshot(BASE2, base_seq + 1, [])
    check("交错 C：8 轮并发交错全部满足一致性不变量（阻断或对账必报，二者必居其一）",
          not violations, str(violations))

    print(f"\n== 结果: {passed} 通过, {failed} 失败")
    if failed:
        sys.exit(1)
    print("== 全部通过。可接着做重启持久化核对（leases/request_log/快照 存于共享 SQLite）。")


if __name__ == "__main__":
    main()
