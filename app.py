import asyncio
import json
import os
import re
import socket
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List
from urllib.parse import urlparse
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

DOCKER_SOCK = "/var/run/docker.sock"
os.makedirs("data", exist_ok=True)
DB_FILE = os.getenv("DB_FILE", "data/monitor.db")

# ==================== 1. 初始化与平滑升级 SQLite 数据库 ====================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS pull_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        image TEXT, status TEXT, total_bytes INTEGER, duration_sec REAL,
        cached_layers INTEGER DEFAULT 0, total_layers INTEGER DEFAULT 0, created_at TEXT
    )''')
    for col in ["cached_layers INTEGER DEFAULT 0", "total_layers INTEGER DEFAULT 0"]:
        try:
            c.execute(f"ALTER TABLE pull_history ADD COLUMN {col}")
        except Exception:
            pass

    c.execute('''CREATE TABLE IF NOT EXISTS network_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        image TEXT, stage TEXT, method TEXT, target_host TEXT, url TEXT, status_code INTEGER,
        latency_ms REAL, size_bytes INTEGER, detail TEXT, extra_json TEXT, created_at TEXT
    )''')
    for col in ["target_host TEXT DEFAULT ''", "extra_json TEXT DEFAULT '{}'"]:
        try:
            c.execute(f"ALTER TABLE network_logs ADD COLUMN {col}")
        except Exception:
            pass

    c.execute('''CREATE TABLE IF NOT EXISTS gantt_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stack_name TEXT, stage_type TEXT, action TEXT, target TEXT,
        start_sec REAL, duration_sec REAL, status TEXT, created_at TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS kv_settings (
        k TEXT PRIMARY KEY, v TEXT
    )''')
    conn.commit()
    conn.close()

init_db()

def get_setting(key: str, default: str = "") -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT v FROM kv_settings WHERE k = ?", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else default

def set_setting(key: str, val: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO kv_settings (k, v) VALUES (?, ?)", (key, val))
    conn.commit()
    conn.close()

# ==================== 2. WebSocket 广播与任务中止控制器 ====================
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

manager = ConnectionManager()

# 保存当前活跃的拉取与抓包协程，用于支持【停止拉取 (ABORT)】按钮
active_pull_controller: Dict[str, List[asyncio.Task]] = {"tasks": []}
gantt_session = {"start_ts": 0.0, "last_ts": 0.0}

async def record_gantt_item(stack_name: str, stage_type: str, action: str, target: str, duration_sec: float, status: str = "OK", custom_start_ts: float = None):
    now = time.time()
    event_start = custom_start_ts if custom_start_ts else (now - duration_sec)
    if gantt_session["start_ts"] == 0.0 or (event_start - gantt_session["last_ts"] > 60.0):
        gantt_session["start_ts"] = event_start
    gantt_session["last_ts"] = now

    start_sec = round(max(0.0, event_start - gantt_session["start_ts"]), 2)
    duration_sec = round(max(0.05, duration_sec), 2)
    item = {
        "stack_name": stack_name or "宿主机/默认",
        "stage_type": stage_type,
        "action": action,
        "target": target,
        "start_sec": start_sec,
        "duration_sec": duration_sec,
        "status": status,
        "created_at": datetime.now().strftime("%H:%M:%S")
    }
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO gantt_logs (stack_name, stage_type, action, target, start_sec, duration_sec, status, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
              (item["stack_name"], item["stage_type"], item["action"], item["target"],
               item["start_sec"], item["duration_sec"], item["status"], item["created_at"]))
    conn.commit()
    conn.close()
    await manager.broadcast({"type": "gantt_event", "data": item})

def save_net_log(log: dict):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO network_logs 
        (image, stage, method, target_host, url, status_code, latency_ms, size_bytes, detail, extra_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
        log["image"], log["stage"], log["method"], log.get("target_host", ""),
        log["url"], log["status_code"], log["latency_ms"], log["size_bytes"],
        log["detail"], json.dumps(log.get("extra", {}), ensure_ascii=False), log["created_at"]
    ))
    conn.commit()
    conn.close()

# ==================== 3. 后台常驻协程：原生监听 Docker /events ====================
async def listen_docker_events():
    while True:
        try:
            transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCK)
            async with httpx.AsyncClient(transport=transport, base_url="http://localhost", timeout=None) as client:
                async with client.stream("GET", "/events") as resp:
                    async for line in resp.aiter_lines():
                        if not line.strip():
                            continue
                        ev = json.loads(line)
                        ev_type = ev.get("Type", "")
                        action = ev.get("Action", "")
                        actor = ev.get("Actor", {})
                        attrs = actor.get("Attributes", {})
                        stack = attrs.get("com.docker.compose.project") or attrs.get("io.portainer.stack.name") or "宿主机事件"

                        if ev_type == "network" and action in ("create", "connect", "destroy"):
                            name = attrs.get("name", actor.get("ID", "")[:12])
                            await record_gantt_item(stack, "NETWORK", f"网络:{action}", name, 0.12)
                        elif ev_type == "volume" and action in ("create", "mount"):
                            name = actor.get("ID", "")[:16]
                            await record_gantt_item(stack, "VOLUME", f"数据卷:{action}", name, 0.10)
                        elif ev_type == "container" and action in ("create", "start", "die", "stop"):
                            name = attrs.get("name", actor.get("ID", "")[:12])
                            await record_gantt_item(stack, "CONTAINER", f"容器:{action}", name, 0.25)
                        elif ev_type == "image" and action in ("pull", "delete"):
                            name = attrs.get("name", actor.get("ID", "")[:16])
                            await record_gantt_item(stack, "PULL", f"镜像:{action}", name, 0.30)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(listen_docker_events())
    yield
    task.cancel()

app = FastAPI(title="Docker Pull Telemetry & Network Inspector", lifespan=lifespan)

# ==================== 4. 核心模块 A：OCI 5阶段流水线与网络抓包瀑布流 ====================
def parse_image_ref(image_input: str):
    img = image_input.strip()
    registry = "registry-1.docker.io"
    parts = img.split("/")
    if len(parts) > 1 and ("." in parts[0] or ":" in parts[0] or parts[0] == "localhost"):
        registry = parts[0]
        repo_part = "/".join(parts[1:])
    else:
        repo_part = img

    tag = "latest"
    if ":" in repo_part:
        repo_part, tag = repo_part.rsplit(":", 1)
    if registry == "registry-1.docker.io" and "/" not in repo_part:
        repo_part = f"library/{repo_part}"

    scheme = "http" if registry.startswith("127.0.0.1") or registry.startswith("localhost") else "https"
    return f"{scheme}://{registry}", registry, repo_part, tag

def resolve_cdn_info(hostname: str) -> tuple:
    t0 = time.perf_counter()
    try:
        ip = socket.gethostbyname(hostname)
    except Exception:
        ip = "未知IP"
    dns_ms = round((time.perf_counter() - t0) * 1000, 1)

    h_low = hostname.lower()
    if "cloudflare" in h_low or ip.startswith(("104.", "172.6", "162.159")):
        vendor = "Cloudflare R2/CDN"
    elif "amazonaws" in h_low or "s3" in h_low:
        vendor = "AWS S3 全球节点"
    elif "pkg.githubusercontent" in h_low or "ghcr" in h_low or "fastly" in h_low:
        vendor = "Fastly / GitHub CDN"
    elif "aliyuncs" in h_low or "aliyun" in h_low:
        vendor = "阿里云 OSS"
    elif "myqcloud" in h_low or "tencent" in h_low:
        vendor = "腾讯云 COS"
    else:
        vendor = "Registry 官方/边缘节点"
    return ip, dns_ms, vendor

async def push_pipeline_stage(stage_idx: int, status: str, subtitle: str, diagnostic: str):
    await manager.broadcast({
        "type": "pipeline_update",
        "stage_idx": stage_idx,
        "status": status,
        "subtitle": subtitle,
        "diagnostic": diagnostic
    })

async def capture_registry_network(image_input: str):
    registry_base, reg_host, repo, tag = parse_image_ref(image_input)
    reg_ip, reg_dns_ms, reg_vendor = resolve_cdn_info(reg_host)

    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=False, verify=False) as client:
            # --- STAGE 01: 协议握手 ---
            await push_pipeline_stage(1, "RUNNING", f"正在连接 {reg_host}...", f"当前正在执行 [1. 协议握手]：正在连接 Registry V2 端点 ({reg_ip})...")
            t0 = time.perf_counter()
            try:
                r1 = await client.get(f"{registry_base}/v2/")
                ms = round((time.perf_counter() - t0) * 1000, 1)
                www_auth = r1.headers.get("www-authenticate", "")
                pkt1 = {
                    "image": image_input, "stage": "1. 协议握手", "method": "GET",
                    "target_host": reg_host, "url": f"{registry_base}/v2/",
                    "status_code": r1.status_code, "latency_ms": ms, "size_bytes": len(r1.content),
                    "detail": f"握手响应 {r1.status_code} | 鉴权挑战: {www_auth[:48]}...",
                    "extra": {"ip": reg_ip, "dns_ms": reg_dns_ms, "vendor": reg_vendor, "headers": dict(r1.headers)},
                    "created_at": datetime.now().strftime("%H:%M:%S")
                }
                save_net_log(pkt1)
                await manager.broadcast({"type": "net_packet", "data": pkt1})
                await push_pipeline_stage(1, "OK", f"耗时 {ms}ms ({r1.status_code})", f"[1. 协议握手] 完成 ({ms}ms)，正在解析 WWW-Authenticate 鉴权头...")
            except Exception as e:
                pkt_err = {
                    "image": image_input, "stage": "1. 协议握手", "method": "GET",
                    "target_host": reg_host, "url": f"{registry_base}/v2/",
                    "status_code": 0, "latency_ms": 0, "size_bytes": 0,
                    "detail": f"握手超时或阻断: {str(e)}",
                    "extra": {"ip": reg_ip, "dns_ms": reg_dns_ms, "vendor": reg_vendor, "error": str(e)},
                    "created_at": datetime.now().strftime("%H:%M:%S")
                }
                save_net_log(pkt_err)
                await manager.broadcast({"type": "net_packet", "data": pkt_err})
                await push_pipeline_stage(1, "ERROR", "连接超时/阻断", f"卡点告警：[1. 协议握手] 连接 {reg_host} ({reg_ip}) 失败：{str(e)}")
                return

            # --- STAGE 02: Token 鉴权 ---
            token = ""
            if r1.status_code == 401 and "realm=" in www_auth:
                realm_m = re.search(r'realm="([^"]+)"', www_auth)
                svc_m = re.search(r'service="([^"]+)"', www_auth)
                realm_url = realm_m.group(1) if realm_m else "https://auth.docker.io/token"
                auth_host = urlparse(realm_url).hostname or "auth.docker.io"
                auth_ip, auth_dns, auth_vendor = resolve_cdn_info(auth_host)
                svc_param = f"service={svc_m.group(1)}&" if svc_m else ""
                auth_url = f"{realm_url}?{svc_param}scope=repository:{repo}:pull"

                await push_pipeline_stage(2, "RUNNING", f"向 {auth_host} 申请令牌", f"当前正在执行 [2. Token 鉴权]：正在向认证中心 {auth_host} 申请拉取 JWT...")
                t0 = time.perf_counter()
                r2 = await client.get(auth_url)
                ms = round((time.perf_counter() - t0) * 1000, 1)
                if r2.status_code == 200:
                    j = r2.json()
                    token = j.get("token") or j.get("access_token") or ""
                pkt2 = {
                    "image": image_input, "stage": "2. Token 鉴权", "method": "GET",
                    "target_host": auth_host, "url": auth_url,
                    "status_code": r2.status_code, "latency_ms": ms, "size_bytes": len(r2.content),
                    "detail": f"成功签发 Bearer JWT (长 {len(token)} 字符)" if token else f"鉴权失败 ({r2.status_code})",
                    "extra": {"ip": auth_ip, "dns_ms": auth_dns, "vendor": auth_vendor, "headers": dict(r2.headers)},
                    "created_at": datetime.now().strftime("%H:%M:%S")
                }
                save_net_log(pkt2)
                await manager.broadcast({"type": "net_packet", "data": pkt2})
                await push_pipeline_stage(2, "OK" if token else "ERROR", f"签发耗时 {ms}ms", f"[2. Token 鉴权] 完成 ({ms}ms)，开始拉取镜像 Manifest 清单...")
            else:
                await push_pipeline_stage(2, "OK", "免密仓库直通", "[2. Token 鉴权] 仓库无需 Bearer Token，直接进入 Manifest 解析...")

            # --- STAGE 03: Manifest 清单 ---
            await push_pipeline_stage(3, "RUNNING", "正在拉取分片索引表", f"当前正在执行 [3. Manifest 清单]：正在获取 {repo}:{tag} 的分片拓扑结构...")
            headers = {
                "Accept": "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json"
            }
            if token:
                headers["Authorization"] = f"Bearer {token}"

            manifest_url = f"{registry_base}/v2/{repo}/manifests/{tag}"
            t0 = time.perf_counter()
            r3 = await client.get(manifest_url, headers=headers)
            ms = round((time.perf_counter() - t0) * 1000, 1)
            rate_rem = r3.headers.get("ratelimit-remaining", "")

            manifest_data = r3.json() if r3.status_code == 200 else {}
            if "manifests" in manifest_data and len(manifest_data["manifests"]) > 0:
                sub_digest = manifest_data["manifests"][0].get("digest")
                r3_sub = await client.get(f"{registry_base}/v2/{repo}/manifests/{sub_digest}", headers=headers)
                if r3_sub.status_code == 200:
                    manifest_data = r3_sub.json()
                    if not rate_rem:
                        rate_rem = r3_sub.headers.get("ratelimit-remaining", "")

            clean_rate = rate_rem.split(";")[0] if ";" in rate_rem else (rate_rem or "-")
            layers = manifest_data.get("layers", [])
            total_manifest_bytes = sum(l.get("size", 0) for l in layers)
            pkt3 = {
                "image": image_input, "stage": "3. Manifest 清单", "method": "GET",
                "target_host": reg_host, "url": manifest_url,
                "status_code": r3.status_code, "latency_ms": ms, "size_bytes": len(r3.content),
                "detail": f"共 {len(layers)} 个分片层 (压缩总计 {round(total_manifest_bytes/1048576, 2)} MB) | 配额: {clean_rate}",
                "extra": {"ip": reg_ip, "dns_ms": reg_dns_ms, "vendor": reg_vendor, "rate_limit": clean_rate, "headers": dict(r3.headers)},
                "created_at": datetime.now().strftime("%H:%M:%S"),
                "rate_limit": clean_rate
            }
            save_net_log(pkt3)
            await manager.broadcast({"type": "net_packet", "data": pkt3})
            if r3.status_code == 429:
                await push_pipeline_stage(3, "ERROR", "429 配额耗尽", "卡点告警：触发 Docker Hub [429 Too Many Requests] 拉取限流！")
                return
            await push_pipeline_stage(3, "OK", f"解析出 {len(layers)} 个分片层", f"[3. Manifest 清单] 解析成功 ({len(layers)} 层)，正在探测 CDN 307 加速节点...")

            # --- STAGE 04: CDN 307 寻址 ---
            await push_pipeline_stage(4, "RUNNING", f"正在探测 {min(5, len(layers))} 个分片路由", "当前正在执行 [4. CDN 307 寻址]：正在嗅探分片 Blob 重定向后的真实 CDN 节点与 IP...")
            slowest_ms = 0.0
            slowest_host = ""
            for idx, layer in enumerate(layers[:5]):
                digest = layer.get("digest", "")
                layer_size = layer.get("size", 0)
                blob_url = f"{registry_base}/v2/{repo}/blobs/{digest}"
                t0 = time.perf_counter()
                r4 = await client.get(blob_url, headers={"Authorization": f"Bearer {token}"} if token else {})
                ms = round((time.perf_counter() - t0) * 1000, 1)
                cdn_loc = r4.headers.get("location", "")
                if cdn_loc:
                    cdn_host = urlparse(cdn_loc).hostname or reg_host
                    cdn_ip, dns_ms, vendor = resolve_cdn_info(cdn_host)
                    detail_str = f"分片 {digest[:15]}... ({round(layer_size/1048576, 2)}MB) -> [{vendor}] {cdn_ip} (DNS {dns_ms}ms)"
                else:
                    cdn_host = reg_host
                    cdn_ip, dns_ms, vendor = reg_ip, reg_dns_ms, reg_vendor
                    detail_str = f"分片 {digest[:15]}... 源站直连 ({r4.status_code})"

                if ms > slowest_ms:
                    slowest_ms = ms
                    slowest_host = f"{cdn_host} ({cdn_ip})"

                pkt4 = {
                    "image": image_input, "stage": f"4. CDN 寻址 #{idx+1}", "method": "GET",
                    "target_host": cdn_host, "url": cdn_loc or blob_url,
                    "status_code": r4.status_code, "latency_ms": ms, "size_bytes": layer_size,
                    "detail": detail_str,
                    "extra": {"ip": cdn_ip, "dns_ms": dns_ms, "vendor": vendor, "digest": digest, "cdn_url": cdn_loc, "headers": dict(r4.headers)},
                    "created_at": datetime.now().strftime("%H:%M:%S")
                }
                save_net_log(pkt4)
                await manager.broadcast({"type": "net_packet", "data": pkt4})

            await push_pipeline_stage(4, "OK", f"已锁定 CDN ({slowest_ms}ms)", f"[4. CDN 307 寻址] 路由探测完成，最高延迟节点: {slowest_host} ({slowest_ms}ms)")
            await push_pipeline_stage(5, "RUNNING", "并发下载与解压落盘", f"当前正在执行 [5. 分片并发传输]：Docker 引擎正在并发拉取并解压落盘 (最高节点 RTT {slowest_ms}ms)...")
    except asyncio.CancelledError:
        await push_pipeline_stage(5, "ERROR", "已手动中止 (ABORT)", "操作通知：用户已点击 [停止拉取 (ABORT)]，网络抓包与分片流已强制中断。")
        raise

# ==================== 5. 核心模块 B：实时分片甘特图流解析 + ABORT 停止接口 ====================
def build_pull_summary(image: str, source: str, state_tag: str, status_text: str, start_ts: float, layers: Dict[str, dict]) -> dict:
    now = time.time()
    elapsed_sec = round(max(0.1, now - start_ts), 1)
    layer_list = []
    for l in layers.values():
        q_sec = l["q_sec"]
        dl_sec = l["dl_sec"]
        ext_sec = l["ext_sec"]
        if l["phase"] == "QUEUE":
            q_sec = round(now - l["first_ts"], 2)
        elif l["phase"] == "DL":
            dl_sec = round(now - l["dl_start_ts"], 2)
        elif l["phase"] == "EXT":
            ext_sec = round(now - l["ext_start_ts"], 2)

        layer_list.append({
            "id": l["id"],
            "status": l["status"],
            "phase": l["phase"],
            "current": l["current"],
            "total": l["total"],
            "percent": l["percent"],
            "speed_bps": l["speed_bps"],
            "q_sec": q_sec,
            "dl_sec": dl_sec,
            "ext_sec": ext_sec
        })

    total_layers = len(layer_list)
    cached_layers = sum(1 for l in layer_list if l["phase"] == "CACHED")
    completed_layers = sum(1 for l in layer_list if l["phase"] in ("DONE", "CACHED"))
    total_speed_bps = sum(l.get("speed_bps", 0.0) for l in layer_list)

    known_total_bytes = sum(l.get("total", 0) for l in layer_list if l.get("total", 0) > 0)
    known_curr_bytes = sum(l.get("current", 0) for l in layer_list if l.get("total", 0) > 0)

    if known_total_bytes > 0:
        overall_pct = round((known_curr_bytes / known_total_bytes) * 100, 1)
    elif total_layers > 0:
        overall_pct = round((completed_layers / total_layers) * 100, 1)
    else:
        overall_pct = 0.0

    rem_bytes = max(0, known_total_bytes - known_curr_bytes)
    eta_sec = round(rem_bytes / total_speed_bps, 1) if (total_speed_bps > 0 and rem_bytes > 0) else 0.0

    return {
        "type": "pull_progress",
        "image": image,
        "source": source,
        "state_tag": state_tag,
        "message": status_text,
        "elapsed_sec": elapsed_sec,
        "total_speed_bps": round(total_speed_bps, 1),
        "overall_percent": min(100.0, overall_pct),
        "downloaded_bytes": known_curr_bytes,
        "total_bytes": known_total_bytes,
        "eta_sec": eta_sec,
        "cached_layers": cached_layers,
        "completed_layers": completed_layers,
        "total_layers": total_layers,
        "layers": layer_list
    }

class PullRequest(BaseModel):
    image: str
    source: str = "面板手动触发"

@app.post("/api/pull")
async def start_pull(req: PullRequest):
    for t in active_pull_controller["tasks"]:
        if not t.done():
            t.cancel()
    active_pull_controller["tasks"] = []

    image = req.image.strip()
    if ":" not in image and "@" not in image:
        image = f"{image}:latest"

    t1 = asyncio.create_task(run_pull_stream(image, req.source))
    t2 = asyncio.create_task(capture_registry_network(image))
    active_pull_controller["tasks"] = [t1, t2]
    return {"status": "started", "image": image}

@app.post("/api/pull/abort")
async def abort_active_pull():
    cancelled_cnt = 0
    for t in active_pull_controller["tasks"]:
        if not t.done():
            t.cancel()
            cancelled_cnt += 1
    active_pull_controller["tasks"] = []
    await manager.broadcast({
        "type": "pull_aborted",
        "message": "已由用户手动强制中止 (ABORTED)"
    })
    return {"status": "aborted", "cancelled_tasks": cancelled_cnt}

def update_layer_state(layers: Dict[str, dict], speed_tracker: Dict[str, tuple], layer_id: str, status_text: str, detail: dict, start_ts: float):
    now = time.time()
    curr = detail.get("current", 0)
    total = detail.get("total", 0)
    if layer_id not in layers:
        layers[layer_id] = {
            "id": layer_id, "status": status_text, "phase": "QUEUE",
            "current": 0, "total": 0, "percent": 0.0, "speed_bps": 0.0,
            "first_ts": now, "dl_start_ts": now, "ext_start_ts": now,
            "q_sec": round(now - start_ts, 2), "dl_sec": 0.0, "ext_sec": 0.0
        }
    item = layers[layer_id]
    item["status"] = status_text

    if status_text == "Already exists":
        item["phase"] = "CACHED"
        item["percent"] = 100.0
        item["speed_bps"] = 0.0
    elif status_text == "Downloading":
        if item["phase"] == "QUEUE":
            item["q_sec"] = round(now - item["first_ts"], 2)
            item["dl_start_ts"] = now
        item["phase"] = "DL"
        if total > 0:
            prev_b, prev_t = speed_tracker.get(layer_id, (curr, now))
            dt = now - prev_t
            if dt >= 0.25 and curr >= prev_b:
                item["speed_bps"] = round((curr - prev_b) / dt, 1)
                speed_tracker[layer_id] = (curr, now)
            elif layer_id not in speed_tracker:
                speed_tracker[layer_id] = (curr, now)
            item["current"] = curr
            item["total"] = total
            item["percent"] = round((curr / total) * 100, 1)
    elif status_text in ("Verifying Checksum", "Download complete"):
        if item["phase"] == "DL":
            item["dl_sec"] = round(now - item["dl_start_ts"], 2)
        item["speed_bps"] = 0.0
        item["percent"] = 100.0
    elif status_text == "Extracting":
        if item["phase"] != "EXT":
            if item["dl_sec"] == 0.0:
                item["dl_sec"] = round(now - item["dl_start_ts"], 2)
            item["ext_start_ts"] = now
        item["phase"] = "EXT"
        item["speed_bps"] = 0.0
        if total > 0:
            item["current"] = curr
            item["total"] = total
            item["percent"] = round((curr / total) * 100, 1)
    elif status_text == "Pull complete":
        if item["phase"] == "EXT":
            item["ext_sec"] = round(now - item["ext_start_ts"], 2)
        item["phase"] = "DONE"
        item["percent"] = 100.0
        item["speed_bps"] = 0.0
        if item["total"] > 0:
            item["current"] = item["total"]

async def run_pull_stream(image: str, source: str = "面板手动触发"):
    start_ts = time.time()
    layers: Dict[str, dict] = {}
    speed_tracker: Dict[str, tuple] = {}
    last_broadcast = 0.0
    status_result = "成功"

    await manager.broadcast({
        "type": "pull_start",
        "image": image,
        "source": source,
        "started_at": datetime.now().strftime("%H:%M:%S")
    })

    transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCK)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://localhost", timeout=None) as client:
            async with client.stream("POST", "/images/create", params={"fromImage": image}) as resp:
                async for line in resp.aiter_lines():
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    layer_id = data.get("id")
                    status_text = data.get("status", "")
                    detail = data.get("progressDetail", {})
                    now = time.time()

                    if layer_id and len(layer_id) == 12:
                        update_layer_state(layers, speed_tracker, layer_id, status_text, detail, start_ts)

                    if (now - last_broadcast >= 0.12) or status_text in ("Pull complete", "Already exists"):
                        await manager.broadcast(build_pull_summary(image, source, "DOWNLOADING", status_text, start_ts, layers))
                        last_broadcast = now

        await push_pipeline_stage(5, "OK", "全部分片落盘完成", f"[5. 分片并发传输] 镜像 {image} 所有分片拉取与解压全部完成！")
    except asyncio.CancelledError:
        status_result = "已中止(ABORT)"
        await record_gantt_item(source, "PULL", "手动中止拉取", image, round(time.time() - start_ts, 2), "ABORTED", custom_start_ts=start_ts)
        raise
    except Exception as e:
        status_result = f"失败: {str(e)}"
        await push_pipeline_stage(5, "ERROR", "拉取异常中断", f"异常告警：{str(e)}")

    duration = round(time.time() - start_ts, 2)
    total_bytes = sum(l.get("total", 0) for l in layers.values())
    cached_cnt = sum(1 for l in layers.values() if l["phase"] == "CACHED")
    total_cnt = len(layers)

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''INSERT INTO pull_history 
                 (image, status, total_bytes, duration_sec, cached_layers, total_layers, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (image, status_result, total_bytes, duration, cached_cnt, total_cnt, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()

    await record_gantt_item(source, "PULL", "拉取镜像分片流", image, duration, status_result, custom_start_ts=start_ts)
    await manager.broadcast(build_pull_summary(image, source, "COMPLETED", "拉取完成", start_ts, layers))
    await manager.broadcast({
        "type": "pull_done",
        "image": image,
        "duration": duration,
        "total_bytes": total_bytes,
        "status": status_result
    })

# ==================== 6. 核心模块 C：/v2/ 流式 Mirror 网关（零 OOM 拦截终端 docker pull） ====================
HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "content-length", "content-encoding"}
mirror_active_layers: Dict[str, dict] = {}
mirror_start_ts: float = time.time()

@app.api_route("/v2/{path:path}", methods=["GET", "HEAD"])
async def registry_mirror_streaming_gateway(path: str, request: Request):
    global mirror_start_ts
    target_url = f"https://registry-1.docker.io/v2/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    t0 = time.perf_counter()
    req_headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "connection")}

    if "/blobs/sha256:" in path and request.method == "GET":
        repo_name = path.split("/blobs/")[0]
        digest_short = path.split("sha256:")[-1][:12]
        now_init = time.time()
        if not mirror_active_layers:
            mirror_start_ts = now_init

        async def stream_blob():
            async with httpx.AsyncClient(follow_redirects=True, timeout=None) as client:
                async with client.stream("GET", target_url, headers=req_headers) as resp:
                    total = int(resp.headers.get("content-length", 0))
                    curr = 0
                    last_t = time.time()
                    last_b = 0
                    mirror_active_layers[digest_short] = {
                        "id": digest_short, "status": "Downloading", "phase": "DL",
                        "current": 0, "total": total, "percent": 0.0, "speed_bps": 0.0,
                        "first_ts": now_init, "dl_start_ts": now_init, "ext_start_ts": now_init,
                        "q_sec": 0.05, "dl_sec": 0.0, "ext_sec": 0.0
                    }
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        curr += len(chunk)
                        now = time.time()
                        if now - last_t >= 0.2:
                            spd_bps = round((curr - last_b) / (now - last_t), 1)
                            pct = round((curr / total) * 100, 1) if total > 0 else 0.0
                            mirror_active_layers[digest_short].update({
                                "current": curr, "percent": pct, "speed_bps": spd_bps,
                                "dl_sec": round(now - now_init, 2)
                            })
                            await manager.broadcast(build_pull_summary(repo_name, "CLI 终端 Mirror", "DOWNLOADING", "Downloading", mirror_start_ts, mirror_active_layers))
                            last_t, last_b = now, curr
                        yield chunk

                    mirror_active_layers[digest_short].update({
                        "status": "Pull complete", "phase": "DONE", "current": total, "percent": 100.0, "speed_bps": 0.0
                    })
                    await manager.broadcast(build_pull_summary(repo_name, "CLI 终端 Mirror", "COMPLETED", "Pull complete", mirror_start_ts, mirror_active_layers))

        return StreamingResponse(stream_blob(), media_type="application/octet-stream")

    async with httpx.AsyncClient(follow_redirects=False, timeout=20.0) as client:
        resp = await client.request(request.method, target_url, headers=req_headers)
        ms = round((time.perf_counter() - t0) * 1000, 1)
        pkt = {
            "image": path.split("/")[1] if "/" in path else "CLI终端拉取",
            "stage": "Mirror 网关拦截",
            "method": request.method,
            "target_host": "registry-1.docker.io",
            "url": f"/v2/{path[:42]}",
            "status_code": resp.status_code,
            "latency_ms": ms,
            "size_bytes": int(resp.headers.get("content-length", 0)),
            "detail": f"终端直拉拦截 -> 状态 {resp.status_code}",
            "extra": {"ip": "127.0.0.1", "dns_ms": 0.1, "vendor": "本地 Mirror 网关"},
            "created_at": datetime.now().strftime("%H:%M:%S")
        }
        save_net_log(pkt)
        await manager.broadcast({"type": "net_packet", "data": pkt})
        clean_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_HEADERS}
        return Response(content=resp.content, status_code=resp.status_code, headers=clean_headers)

# ==================== 7. 核心模块 D：Portainer Token 绑定、堆栈解析与一键重部署 ====================
class PortainerConfigModel(BaseModel):
    url: str
    token: str

@app.get("/api/portainer/config")
async def get_portainer_config():
    url = get_setting("portainer_url", "")
    token = get_setting("portainer_token", "")
    return {"url": url, "has_token": bool(token), "token_masked": (token[:8] + "..." + token[-4:]) if len(token) > 12 else token}

@app.post("/api/portainer/config")
async def save_portainer_config(cfg: PortainerConfigModel):
    clean_url = cfg.url.strip().rstrip("/")
    set_setting("portainer_url", clean_url)
    if cfg.token.strip() and "..." not in cfg.token:
        set_setting("portainer_token", cfg.token.strip())
    return {"status": "ok", "url": clean_url}

@app.get("/api/portainer/stacks")
async def list_portainer_stacks():
    base_url = get_setting("portainer_url", "")
    token = get_setting("portainer_token", "")
    if not base_url or not token:
        return {"configured": False, "stacks": []}

    headers = {"X-API-Key": token}
    try:
        async with httpx.AsyncClient(verify=False, timeout=12.0) as client:
            r = await client.get(f"{base_url}/api/stacks", headers=headers)
            if r.status_code != 200:
                return {"configured": True, "error": f"Portainer 响应异常 ({r.status_code}): {r.text[:100]}", "stacks": []}

            stacks = r.json()
            result = []
            for s in stacks:
                sid = s.get("Id")
                f_resp = await client.get(f"{base_url}/api/stacks/{sid}/file", headers=headers)
                yaml_content = f_resp.json().get("StackFileContent", "") if f_resp.status_code == 200 else ""
                images = re.findall(r'^\s*image:\s*["\']?([^\s"\'#]+)["\']?', yaml_content, re.MULTILINE)
                result.append({
                    "id": sid,
                    "name": s.get("Name"),
                    "endpoint_id": s.get("EndpointId"),
                    "status": "ACTIVE" if s.get("Status") == 1 else "INACTIVE",
                    "images": list(dict.fromkeys(images)),
                    "yaml": yaml_content
                })
            return {"configured": True, "stacks": result}
    except Exception as e:
        return {"configured": True, "error": f"连接 Portainer 失败: {str(e)}", "stacks": []}

@app.post("/api/portainer/stacks/{stack_id}/redeploy")
async def redeploy_portainer_stack(stack_id: int):
    base_url = get_setting("portainer_url", "")
    token = get_setting("portainer_token", "")
    if not base_url or not token:
        raise HTTPException(status_code=400, detail="请先配置 Portainer URL 与 Token")

    headers = {"X-API-Key": token}
    async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
        s_resp = await client.get(f"{base_url}/api/stacks/{stack_id}", headers=headers)
        f_resp = await client.get(f"{base_url}/api/stacks/{stack_id}/file", headers=headers)
        if s_resp.status_code != 200 or f_resp.status_code != 200:
            raise HTTPException(status_code=400, detail="读取 Portainer 堆栈详情失败")

        stack_info = s_resp.json()
        stack_name = stack_info.get("Name", f"Stack-{stack_id}")
        endpoint_id = stack_info.get("EndpointId", 1)
        env_list = stack_info.get("Env") or []
        yaml_content = f_resp.json().get("StackFileContent", "")
        images = list(dict.fromkeys(re.findall(r'^\s*image:\s*["\']?([^\s"\'#]+)["\']?', yaml_content, re.MULTILINE)))

    async def execute_visual_redeploy():
        gantt_session["start_ts"] = time.time()
        gantt_session["last_ts"] = time.time()
        await record_gantt_item(stack_name, "CONTAINER", "启动堆栈重部署", f"共 {len(images)} 个镜像", 0.1)

        for img in images:
            await asyncio.gather(
                run_pull_stream(img, source=f"Portainer:{stack_name}"),
                capture_registry_network(img)
            )

        t0 = time.time()
        async with httpx.AsyncClient(verify=False, timeout=60.0) as client:
            put_url = f"{base_url}/api/stacks/{stack_id}?endpointId={endpoint_id}"
            payload = {"stackFileContent": yaml_content, "env": env_list, "prune": False, "pullImage": False}
            r = await client.put(put_url, headers=headers, json=payload)
            dur = round(time.time() - t0, 2)
            st = "OK" if r.status_code == 200 else f"ERR-{r.status_code}"
            await record_gantt_item(stack_name, "CONTAINER", "堆栈容器重建完成", stack_name, dur, st, custom_start_ts=t0)

    t_redeploy = asyncio.create_task(execute_visual_redeploy())
    active_pull_controller["tasks"] = [t_redeploy]
    return {"status": "started", "stack_name": stack_name, "images": images}

# ==================== 8. 宿主机资产清理、/system/df 透视与统计接口 ====================
@app.post("/api/gantt/clear")
async def clear_gantt_logs():
    gantt_session["start_ts"] = 0.0
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM gantt_logs")
    conn.commit()
    conn.close()
    return {"status": "cleared"}

@app.delete("/api/images/{image_ref:path}")
async def delete_local_image(image_ref: str):
    transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCK)
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost", timeout=15.0) as client:
        r = await client.delete(f"/images/{image_ref}?force=true")
        if r.status_code in (200, 204):
            await record_gantt_item("镜像工作台", "PULL", "强制移除镜像", image_ref, 0.15)
            return {"status": "deleted", "image": image_ref}
        raise HTTPException(status_code=r.status_code, detail=r.text)

@app.post("/api/host/prune")
async def prune_docker_storage():
    transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCK)
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost", timeout=30.0) as client:
        r_img, r_cache = await asyncio.gather(
            client.post("/images/prune"),
            client.post("/build/prune")
        )
        reclaimed = 0
        if r_img.status_code == 200:
            reclaimed += r_img.json().get("SpaceReclaimed", 0)
        if r_cache.status_code == 200:
            reclaimed += r_cache.json().get("SpaceReclaimed", 0)
        reclaimed_mb = round(reclaimed / 1048576, 2)
        await record_gantt_item("镜像工作台", "VOLUME", "清理悬空镜像(PRUNE)", f"释放 {reclaimed_mb} MB", 0.25)
        return {"reclaimed_mb": reclaimed_mb}

@app.get("/api/host-info")
async def get_host_docker_info():
    transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCK)
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost", timeout=15.0) as client:
        info_r, ver_r, cont_r, img_r, df_r = await asyncio.gather(
            client.get("/info"), client.get("/version"),
            client.get("/containers/json?all=true"), client.get("/images/json"),
            client.get("/system/df")
        )
        info, ver, containers, images = info_r.json(), ver_r.json(), cont_r.json(), img_r.json()
        df = df_r.json() if df_r.status_code == 200 else {}

        img_bytes = sum(i.get("Size", 0) for i in (df.get("Images") or []))
        cont_bytes = sum(c.get("SizeRw", 0) for c in (df.get("Containers") or []))
        vol_bytes = sum(v.get("UsageData", {}).get("Size", 0) for v in (df.get("Volumes") or []) if v.get("UsageData"))
        cache_bytes = sum(b.get("Size", 0) for b in (df.get("BuildCache") or []))
        reclaim_bytes = sum(b.get("Size", 0) for b in (df.get("BuildCache") or []) if not b.get("InUse"))

        return {
            "host": {
                "name": info.get("Name"), "os": info.get("OperatingSystem"),
                "kernel": info.get("KernelVersion"), "arch": info.get("Architecture"),
                "cpus": info.get("NCPU"), "mem_gb": round(info.get("MemTotal", 0) / (1024**3), 2),
                "docker_ver": ver.get("Version"), "api_ver": ver.get("ApiVersion"),
                "driver": info.get("Driver"), "cgroup": info.get("CgroupVersion", "1"),
                "root_dir": info.get("DockerRootDir"),
                "mirrors": info.get("RegistryConfig", {}).get("Mirrors") or [],
                "containers_running": info.get("ContainersRunning", 0),
                "containers_stopped": info.get("ContainersStopped", 0),
                "images_count": len(images)
            },
            "df": {
                "images_mb": round(img_bytes / 1048576, 1),
                "containers_mb": round(cont_bytes / 1048576, 1),
                "volumes_mb": round(vol_bytes / 1048576, 1),
                "build_cache_mb": round(cache_bytes / 1048576, 1),
                "reclaimable_mb": round(reclaim_bytes / 1048576, 1)
            },
            "containers": [{
                "id": c.get("Id", "")[:12],
                "name": (c.get("Names", ["/未知"])[0]).lstrip("/"),
                "image": c.get("Image"), "state": c.get("State"), "status": c.get("Status"),
                "stack": c.get("Labels", {}).get("com.docker.compose.project", "-")
            } for c in containers[:40]],
            "images": [{
                "id": i.get("Id", "").replace("sha256:", "")[:12],
                "tags": i.get("RepoTags") or ["<none>:<none>"],
                "size_mb": round(i.get("Size", 0) / 1048576, 1),
                "created": datetime.fromtimestamp(i.get("Created", 0)).strftime("%Y-%m-%d %H:%M")
            } for i in images[:50]]
        }

@app.get("/api/stats")
async def get_stats():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as cnt, COALESCE(SUM(total_bytes), 0) as bytes FROM pull_history")
    pull_sum = dict(c.fetchone())
    c.execute("SELECT COUNT(*) as cnt, COALESCE(AVG(latency_ms), 0) as avg_ms FROM network_logs WHERE status_code > 0")
    net_sum = dict(c.fetchone())
    c.execute("SELECT * FROM pull_history ORDER BY id DESC LIMIT 12")
    history = [dict(r) for r in c.fetchall()]
    c.execute("SELECT * FROM network_logs ORDER BY id DESC LIMIT 40")
    raw_packets = [dict(r) for r in c.fetchall()]
    for p in raw_packets:
        try:
            p["extra"] = json.loads(p.get("extra_json") or "{}")
        except Exception:
            p["extra"] = {}
    c.execute("SELECT * FROM gantt_logs ORDER BY id ASC LIMIT 45")
    gantt_items = [dict(r) for r in c.fetchall()]
    conn.close()
    return {
        "total_pulls": pull_sum["cnt"],
        "total_mb": round(pull_sum["bytes"] / 1048576, 2),
        "total_packets": net_sum["cnt"],
        "avg_latency_ms": round(net_sum["avg_ms"], 1),
        "history": history,
        "packets": raw_packets,
        "gantt": gantt_items
    }

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)

@app.get("/", response_class=HTMLResponse)
async def index_page():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

# ==================== 9. 放在文件最末尾：全局兜底 Docker API 透明代理 ====================
# 支持 Portainer 直接填写 192.168.206.4:8001（免 /docker-api 后缀）或带 /docker-api 后缀
@app.api_route("/docker-api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
async def docker_api_transparent_proxy(path: str, request: Request):
    clean_path = path[len("docker-api/"):] if path.startswith("docker-api/") else path
    query = f"?{request.url.query}" if request.url.query else ""
    target_path = f"/{clean_path}{query}"
    body = await request.body()
    req_headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "connection", "content-length")}
    t_start = time.time()

    # 1. 拦截 Portainer / 外部 Docker 客户端的镜像拉取请求 (POST /v1.xx/images/create)
    if "images/create" in clean_path and request.method == "POST":
        from_img = request.query_params.get("fromImage", "")
        tag = request.query_params.get("tag", "")
        full_img = f"{from_img}:{tag}" if (tag and ":" not in from_img) else (from_img or "未知镜像")

        asyncio.create_task(capture_registry_network(full_img))
        await manager.broadcast({"type": "pull_start", "image": full_img, "source": "Portainer 代理", "started_at": datetime.now().strftime("%H:%M:%S")})

        async def tap_pull_stream():
            layers = {}
            speed_tracker = {}
            last_push = 0.0
            transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCK)
            async with httpx.AsyncClient(transport=transport, base_url="http://localhost", timeout=None) as client:
                async with client.stream("POST", target_path, content=body, headers=req_headers) as resp:
                    async for line in resp.aiter_lines():
                        if line.strip():
                            try:
                                data = json.loads(line)
                                lid = data.get("id")
                                st = data.get("status", "")
                                detail = data.get("progressDetail", {})
                                now = time.time()
                                if lid and len(lid) == 12:
                                    update_layer_state(layers, speed_tracker, lid, st, detail, t_start)
                                if now - last_push >= 0.12 or st in ("Pull complete", "Already exists"):
                                    await manager.broadcast(build_pull_summary(full_img, "Portainer 代理", "DOWNLOADING", st, t_start, layers))
                                    last_push = now
                            except Exception:
                                pass
                        yield (line + "\n").encode("utf-8")

            dur = round(time.time() - t_start, 2)
            await push_pipeline_stage(5, "OK", "Portainer 分片拉取完成", f"Portainer 堆栈镜像 {full_img} 拉取完成 (耗时 {dur}s)")
            await record_gantt_item("Portainer代理", "PULL", "代理拉取镜像", full_img, dur, "OK", custom_start_ts=t_start)
            await manager.broadcast({"type": "pull_done", "image": full_img, "duration": dur, "total_bytes": 0, "status": "完成"})

        return StreamingResponse(tap_pull_stream(), media_type="application/json")

    # 2. 转发其他所有 Docker Engine API 请求 (/_ping, /v1.xx/containers, /v1.xx/networks 等)
    transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCK)
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost", timeout=None) as client:
        resp = await client.request(request.method, target_path, content=body, headers=req_headers)
        dur = round(time.time() - t_start, 2)

        if request.method in ("POST", "DELETE", "PUT") and not clean_path.endswith("/json"):
            stage = "CONTAINER"
            if "networks" in clean_path:
                stage = "NETWORK"
            elif "volumes" in clean_path:
                stage = "VOLUME"
            await record_gantt_item("Portainer代理", stage, f"{request.method}", f"/{clean_path[:38]}", dur, str(resp.status_code), custom_start_ts=t_start)

        clean_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_HEADERS}
        return Response(content=resp.content, status_code=resp.status_code, headers=clean_headers)
