#!/usr/bin/env python3
"""快照序号与内容的重启持久化自测（仅依赖标准库）。

流程：
1. 在独立工作目录拉起一个 uvicorn 实例（独立 DB 文件，避免影响开发实例）；
2. 提交两份快照（#1、#2）并做一次同序号冲突/幂等校验；
3. 杀掉进程，再用同一 DB 文件重新拉起；
4. 核对重启后仍读取最后成功提交的序号 #2、占用内容完整，且旧序号继续被拒绝。

用法：python3 scripts/selftest_restart.py
"""
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORK = os.path.join(ROOT, "data", "restart-selftest")
PORT = int(os.environ.get("RESTART_TEST_PORT", "8099"))
BASE = f"http://127.0.0.1:{PORT}"
DB_PATH = os.path.join(WORK, "leases.db")

passed = failed = 0


def check(name, cond, extra=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"    PASS  {name}")
    else:
        failed += 1
        print(f"    FAIL  {name}  {extra}")


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_ready(proc, timeout=25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"uvicorn 提前退出，code={proc.returncode}")
        try:
            with urllib.request.urlopen(BASE + "/api/health", timeout=2) as r:
                if r.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, OSError):
            time.sleep(0.3)
    raise RuntimeError("等待服务就绪超时")


def start_node(instance_id):
    env = os.environ.copy()
    env.update({
        "DB_PATH": DB_PATH,
        "POOL_CIDR": "10.99.0.0/29",
        "INSTANCE_ID": instance_id,
        "ALLOW_RESET": "1",
        "PYTHONPATH": os.path.join(ROOT, "app"),
    })
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
         "--port", str(PORT), "--app-dir", os.path.join(ROOT, "app")],
        cwd=WORK, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, preexec_fn=os.setsid)
    wait_ready(proc)
    return proc


def stop_node(proc):
    if proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=10)


def main():
    if os.path.isdir(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK, exist_ok=True)

    print("-- R1 首次启动并提交快照 #1 / #2")
    proc = start_node("node-A")
    try:
        check("健康检查", req("GET", "/api/health")[0] == 200)
        s, b = req("POST", "/api/snapshots", {"seq": 1, "entries": [
            {"vm_id": "vm-a", "address": "10.99.0.1"}]})
        check("快照 #1 提交成功", s == 201 and b["snapshot_seq"] == 1, f"{s} {b}")
        s, b = req("POST", "/api/snapshots", {"seq": 2, "entries": [
            {"vm_id": "vm-a", "address": "10.99.0.1"},
            {"vm_id": "vm-b", "address": "10.99.0.2"},
        ]})
        check("快照 #2 提交成功", s == 201 and b["snapshot_seq"] == 2 and len(b["entries"]) == 2,
              f"{s} {b}")
        s, b = req("POST", "/api/snapshots", {"seq": 2, "entries": [
            {"vm_id": "vm-other", "address": "10.99.0.3"}]})
        check("同序号异内容被拒(409)", s == 409 and b["detail"]["error"] == "snapshot_seq_conflict",
              f"{s} {b}")
        s, b = req("POST", "/api/snapshots", {"seq": 2, "entries": [
            {"vm_id": "vm-a", "address": "10.99.0.1"},
            {"vm_id": "vm-b", "address": "10.99.0.2"},
        ]})
        check("同序号同内容幂等(200)", s == 200 and b.get("replayed") is True, f"{s} {b}")
        # 同时写一条租约，验证租约数据与快照一并持久化
        s, b = req("POST", "/api/leases",
                   {"vm_id": "vm-c", "request_id": "req-persist", "ttl_seconds": 60})
        check("顺带写入一条租约", s == 201, f"{s} {b}")
        lease_addr = b["lease"]["address"]
    finally:
        print("-- R2 杀掉进程（模拟重启/宕机）")
        stop_node(proc)

    print("-- R3 用同一 DB 文件重新拉起（新实例 node-B）")
    proc = start_node("node-B")
    try:
        check("重启后健康检查（新实例）", req("GET", "/api/health")[1].get("node") == "node-B")
        s, b = req("GET", "/api/snapshots/latest")
        check("重启后仍读到最后成功提交的序号 #2",
              s == 200 and b["snapshot_seq"] == 2 and len(b["entries"]) == 2, f"{s} {b}")
        check("重启后占用内容完整",
              [(e["vm_id"], e["address"]) for e in b["entries"]] ==
              [("vm-a", "10.99.0.1"), ("vm-b", "10.99.0.2")], json.dumps(b["entries"]))
        check("重启后 content_hash/entry_count 保留",
              b.get("entry_count") == 2 and len(b.get("content_hash", "")) == 64, str(b.get("content_hash")))
        snaps = req("GET", "/api/snapshots")[1]
        check("历史快照列表含 #1、#2", [x["seq"] for x in snaps["snapshots"]] == [2, 1],
              json.dumps(snaps))
        s, b = req("POST", "/api/snapshots", {"seq": 1, "entries": []})
        check("重启后旧序号 #1 仍被拒绝(409 snapshot_seq_stale)",
              s == 409 and b["detail"]["error"] == "snapshot_seq_stale"
              and b["detail"]["current_seq"] == 2, f"{s} {b}")
        s, b = req("POST", "/api/snapshots", {"seq": 3, "entries": []})
        check("重启后可用更大序号 #3 继续提交", s == 201 and b["snapshot_seq"] == 3, f"{s} {b}")
        s, b = req("GET", "/api/requests/req-persist")
        check("租约台账一并持久化（req-persist 仍可查）",
              s == 200 and b["lease"]["address"] == lease_addr and b["lease"]["status"] == "active",
              f"{s} {b}")
        # 失败回滚核对：#3 是空快照，提交一个带池外地址的 #4 必须整体失败，#3 原样保留
        s, b = req("POST", "/api/snapshots",
                   {"seq": 4, "entries": [{"vm_id": "vm-x", "address": "8.8.8.8"}]})
        check("池外地址提交被参数层拒绝(422)", s == 422, f"{s} {b}")
        s, b = req("GET", "/api/snapshots/latest")
        check("失败后最后成功提交仍是 #3（上一份完整快照保留）",
              b["snapshot_seq"] == 3, f"{s} {b}")
    finally:
        stop_node(proc)

    print(f"\n== 重启持久化结果: {passed} 通过, {failed} 失败")
    if failed:
        sys.exit(1)
    print("== 全部通过：快照序号/内容随共享 SQLite 持久化，重启后继续使用最后成功提交的序号。")


if __name__ == "__main__":
    main()
