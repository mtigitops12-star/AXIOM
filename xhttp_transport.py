# xhttp_transport.py
# ══════════════════════════════════════════════════════════════════════════════
# ترابرد XHTTP برای IMMINENT Panel — دو مد: packet-up / stream-up
# از همون سیستم quota/connections/max_connections خودِ main.py استفاده می‌کنه
# (بدون هیچ محدودیت جداگانه‌ی IP — طبق درخواست، فقط max_connections سنجیده می‌شه).
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import secrets
import socket
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse

from main import (
    LINKS,
    LINKS_LOCK,
    connections,
    connections_lock,
    connection_sockets,
    link_ip_map,
    stats,
    hourly_traffic,
    daily_traffic,
    error_logs,
    logger,
    parse_proxy_header,
    response_prefix_for_protocol,
    DEFAULT_PROTOCOL,
    check_and_add_usage,
    count_connections_for_link,
    get_request_ip,
    save_db,
    _log_connection_event,
    _fmt_bytes,
)

router = APIRouter()

XHTTP_BUF = 256 * 1024
SESSION_IDLE_TIMEOUT = 30
REAPER_INTERVAL = 10
TCP_CONNECT_TIMEOUT = 10.0
DOWNLINK_QUEUE_MAX = 512

xhttp_sessions: dict = {}
XHTTP_LOCK = asyncio.Lock()


def _tune_socket(writer: asyncio.StreamWriter):
    sock = writer.transport.get_extra_info("socket")
    if not sock:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass


async def _check_link_active(uid: str) -> dict:
    """چک می‌کنه لینک فعال/منقضی‌نشده باشه و برمی‌گردونه، وگرنه 403."""
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None or not link.get("active"):
            raise HTTPException(status_code=403, detail="not authorized")
        return dict(link)


async def _get_or_create_session(uid: str, auth: str, mode: str, session_id: str, ip: str) -> dict:
    async with XHTTP_LOCK:
        sess = xhttp_sessions.get(session_id)
        if sess is not None:
            sess["last_seen"] = time.time()
            return sess

        link = await _check_link_active(uid)
        variant = link.get("variants", {}).get(auth)
        if not variant or not variant.get("enabled") or variant.get("transport") != f"xhttp-{mode}":
            raise HTTPException(status_code=403, detail="not authorized")
        max_conn = link.get("max_connections", 0)
        if max_conn > 0 and await count_connections_for_link(uid) >= max_conn:
            raise HTTPException(status_code=403, detail="max connections reached")

        conn_id = secrets.token_urlsafe(8)
        async with connections_lock:
            connections[conn_id] = {
                "uuid": uid, "ip": ip,
                "connected_at": datetime.now(timezone.utc).isoformat(),
                "bytes": 0,
                "transport": f"xhttp-{mode}",
            }
            connection_sockets.pop(conn_id, None)
            link_ip_map[uid].add(ip)

        await _log_connection_event("connect", link.get("label", uid), uid, ip)

        sess = {
            "uuid": uid, "auth": auth, "mode": mode, "conn_id": conn_id, "ip": ip,
            "writer": None, "tcp_open": False,
            "down_q": asyncio.Queue(maxsize=DOWNLINK_QUEUE_MAX),
            "seq_buf": {}, "next_seq": 0,
            "last_seen": time.time(), "closed": False,
            "resp_prefix": response_prefix_for_protocol(auth),
        }
        xhttp_sessions[session_id] = sess
        return sess


async def _teardown(session_id: str):
    async with XHTTP_LOCK:
        sess = xhttp_sessions.pop(session_id, None)
    if not sess or sess.get("closed"):
        return
    sess["closed"] = True
    writer = sess.get("writer")
    if writer:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
    conn_id = sess.get("conn_id")
    uid = sess.get("uuid")
    ip = sess.get("ip")
    info = None
    async with connections_lock:
        info = connections.pop(conn_id, None)
        connection_sockets.pop(conn_id, None)
        if info and uid and ip:
            has_other = any(c.get("uuid") == uid and c.get("ip") == ip for c in connections.values())
            if not has_other and uid in link_ip_map:
                link_ip_map[uid].discard(ip)
                if not link_ip_map[uid]:
                    link_ip_map.pop(uid, None)
    dq = sess.get("down_q")
    if dq:
        try:
            dq.put_nowait(None)
        except Exception:
            pass
    if info:
        try:
            connected_at = datetime.fromisoformat(info["connected_at"])
            duration_s = max(0, int((datetime.now(timezone.utc) - connected_at).total_seconds()))
        except Exception:
            duration_s = 0
        async with LINKS_LOCK:
            label = LINKS.get(uid, {}).get("label", uid)
        extra = f"duration {duration_s}s, {_fmt_bytes(info.get('bytes', 0))}"
        await _log_connection_event("disconnect", label, uid, ip, extra)
    logger.info(f"closed XHTTP[{sess.get('mode')}] [{session_id[:8]}] total={len(xhttp_sessions)}")


async def _reaper():
    while True:
        await asyncio.sleep(REAPER_INTERVAL)
        now = time.time()
        async with XHTTP_LOCK:
            stale = [sid for sid, s in xhttp_sessions.items()
                     if now - s["last_seen"] > SESSION_IDLE_TIMEOUT and not s.get("tcp_open")]
        for sid in stale:
            await _teardown(sid)


_reaper_started = False


def ensure_reaper():
    global _reaper_started
    if not _reaper_started:
        asyncio.create_task(_reaper())
        _reaper_started = True


def _record_traffic(size: int, conn_id: str):
    stats["total_bytes"] += size
    stats["total_requests"] += 1
    now = datetime.now(timezone.utc)
    hourly_traffic[now.strftime("%Y-%m-%d %H:00")] += size
    daily_traffic[now.strftime("%Y-%m-%d")] += size
    c = connections.get(conn_id)
    if c:
        c["bytes"] += size


async def _pump_tcp_to_queue(session_id: str, uid: str, reader: asyncio.StreamReader, down_q: asyncio.Queue, resp_prefix: bytes = b"\x00\x00"):
    first = True
    try:
        while True:
            data = await reader.read(XHTTP_BUF)
            if not data:
                break
            if not await check_and_add_usage(uid, len(data)):
                break
            async with XHTTP_LOCK:
                sess = xhttp_sessions.get(session_id)
            if sess:
                _record_traffic(len(data), sess["conn_id"])
            payload = (resp_prefix + data) if (first and resp_prefix) else data
            first = False
            await down_q.put(payload)
    except (asyncio.CancelledError, Exception):
        pass
    finally:
        await _teardown(session_id)
        asyncio.create_task(save_db())


async def _open_tcp_for_session(session_id: str, uid: str, sess: dict, first_chunk: bytes):
    auth = sess.get("auth", "vless")
    command, address, port, payload = await parse_proxy_header(auth, first_chunk)
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(address, port), timeout=TCP_CONNECT_TIMEOUT
    )
    _tune_socket(writer)
    if payload:
        writer.write(payload)
        await writer.drain()
    logger.info(f"connect XHTTP[{sess['mode']}] [{session_id[:8]}] -> {address}:{port}")
    sess["writer"] = writer
    sess["tcp_open"] = True
    sess["downlink_task"] = asyncio.create_task(
        _pump_tcp_to_queue(session_id, uid, reader, sess["down_q"], resp_prefix=sess.get("resp_prefix", b"\x00\x00"))
    )


def _downstream_gen(sess: dict):
    async def gen():
        while True:
            chunk = await sess["down_q"].get()
            if chunk is None:
                break
            sess["last_seen"] = time.time()
            yield chunk
    return gen()


_XHTTP_HEADERS = {
    "content-type": "application/grpc",
    "cache-control": "no-cache, no-store",
    "x-accel-buffering": "no",
}


# ══════════════════════════ GET دانلینک (مشترک بین دو مد) ══════════════════════════
@router.get("/xhttp/{auth}/{mode}/{uuid}/{session_id}")
async def xhttp_downlink(auth: str, mode: str, uuid: str, session_id: str, request: Request):
    ensure_reaper()
    if auth not in ("vless", "trojan"):
        raise HTTPException(status_code=404, detail="unknown auth")
    if mode not in ("packet-up", "stream-up"):
        raise HTTPException(status_code=404, detail="unknown mode")
    ip = get_request_ip(request)
    sess = await _get_or_create_session(uuid, auth, mode, session_id, ip)
    if sess.get("closed"):
        raise HTTPException(status_code=404, detail="session closed")
    return StreamingResponse(_downstream_gen(sess), headers=_XHTTP_HEADERS, media_type=_XHTTP_HEADERS["content-type"])


# ══════════════════════════ PACKET-UP (آپلینک با seq) ══════════════════════════
@router.post("/xhttp/{auth}/packet-up/{uuid}/{session_id}/{seq}")
async def packet_up_upload(auth: str, uuid: str, session_id: str, seq: int, request: Request):
    ensure_reaper()
    if auth not in ("vless", "trojan"):
        raise HTTPException(status_code=404, detail="unknown auth")
    ip = get_request_ip(request)
    sess = await _get_or_create_session(uuid, auth, "packet-up", session_id, ip)
    if sess.get("closed"):
        raise HTTPException(status_code=404, detail="session closed")

    sess["last_seen"] = time.time()
    body = await request.body()
    if not body:
        return {"ok": True}

    if not await check_and_add_usage(uuid, len(body)):
        await _teardown(session_id)
        raise HTTPException(status_code=403, detail="quota exceeded")
    _record_traffic(len(body), sess["conn_id"])

    try:
        if sess["writer"] is None:
            # اولین پکت (شامل هدر VLESS/Trojan) ممکنه seq=0 نباشه اگه پکت‌ها خارج از
            # ترتیب برسن؛ بافر کوچیک برای سورت‌کردن seqهای زودرس.
            if seq != 0:
                sess["seq_buf"][seq] = body
                return {"ok": True, "buffered": True}
            await _open_tcp_for_session(session_id, uuid, sess, body)
            nxt = 1
            while nxt in sess["seq_buf"]:
                pending = sess["seq_buf"].pop(nxt)
                sess["writer"].write(pending)
                nxt += 1
            sess["next_seq"] = nxt
            return {"ok": True, "connected": True}

        if seq == sess["next_seq"]:
            sess["writer"].write(body)
            sess["next_seq"] += 1
            while sess["next_seq"] in sess["seq_buf"]:
                pending = sess["seq_buf"].pop(sess["next_seq"])
                sess["writer"].write(pending)
                sess["next_seq"] += 1
        else:
            sess["seq_buf"][seq] = body

        await sess["writer"].drain()
    except Exception as exc:
        error_logs.append({"error": str(exc), "time": datetime.now(timezone.utc).isoformat()})
        await _teardown(session_id)
        raise HTTPException(status_code=502, detail="write failed")

    return {"ok": True}


# ══════════════════════════ STREAM-UP (یک POST پیوسته) ══════════════════════════
@router.post("/xhttp/{auth}/stream-up/{uuid}/{session_id}")
async def stream_up_upload(auth: str, uuid: str, session_id: str, request: Request):
    ensure_reaper()
    if auth not in ("vless", "trojan"):
        raise HTTPException(status_code=404, detail="unknown auth")
    ip = get_request_ip(request)
    sess = await _get_or_create_session(uuid, auth, "stream-up", session_id, ip)
    if sess.get("closed"):
        raise HTTPException(status_code=404, detail="session closed")

    writer = sess["writer"]
    try:
        async for chunk in request.stream():
            if not chunk:
                continue
            sess["last_seen"] = time.time()

            if not await check_and_add_usage(uuid, len(chunk)):
                raise HTTPException(status_code=403, detail="quota exceeded")
            _record_traffic(len(chunk), sess["conn_id"])

            if writer is None:
                await _open_tcp_for_session(session_id, uuid, sess, chunk)
                writer = sess["writer"]
                continue

            writer.write(chunk)
            if writer.transport.get_write_buffer_size() > 2 * 1024 * 1024:
                await writer.drain()
    except HTTPException:
        await _teardown(session_id)
        raise
    except Exception as exc:
        error_logs.append({"error": str(exc), "time": datetime.now(timezone.utc).isoformat()})
        await _teardown(session_id)
        raise HTTPException(status_code=502, detail="stream error")

    return {"ok": True}
