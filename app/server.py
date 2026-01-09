import asyncio
import os
from typing import Any, Dict

from aiohttp import web

from app.raft import RaftNode


async def json_response(payload: Dict[str, Any], status: int = 200) -> web.Response:
    return web.json_response(payload, status=status)


def build_app(node: RaftNode) -> web.Application:
    app = web.Application()

    def leader_redirect(request: web.Request) -> web.Response:
        leader = node.leader_address()
        if not leader:
            return web.json_response({"error": "leader_unknown"}, status=503)
        leader_url = f"http://{leader}{request.rel_url}"
        return web.Response(status=307, headers={"Location": leader_url})

    async def health(_: web.Request) -> web.Response:
        return await json_response({"status": "ok", "role": node.role, "leader": node.leader_id})

    async def get_value(request: web.Request) -> web.Response:
        if not node.is_leader():
            return leader_redirect(request)
        key = request.query.get("key")
        if key is None:
            return await json_response({"error": "key_required"}, status=400)
        value = node.get_value(key)
        return await json_response({"key": key, "value": value})

    async def put_value(request: web.Request) -> web.Response:
        if not node.is_leader():
            return leader_redirect(request)
        data = await request.json()
        key = data.get("key")
        value = data.get("value")
        if key is None:
            return await json_response({"error": "key_required"}, status=400)
        success = await node.replicate_command({"op": "PUT", "key": key, "value": value})
        if not success:
            return await json_response({"error": "replication_failed"}, status=503)
        return await json_response({"status": "ok", "key": key, "value": value})

    async def delete_value(request: web.Request) -> web.Response:
        if not node.is_leader():
            return leader_redirect(request)
        data = await request.json()
        key = data.get("key")
        if key is None:
            return await json_response({"error": "key_required"}, status=400)
        success = await node.replicate_command({"op": "DELETE", "key": key})
        if not success:
            return await json_response({"error": "replication_failed"}, status=503)
        return await json_response({"status": "ok", "key": key})

    async def request_vote(request: web.Request) -> web.Response:
        payload = await request.json()
        data = await node.handle_request_vote(payload)
        return await json_response(data)

    async def append_entries(request: web.Request) -> web.Response:
        payload = await request.json()
        data = await node.handle_append_entries(payload)
        return await json_response(data)

    async def join_cluster(request: web.Request) -> web.Response:
        payload = await request.json()
        peer = payload.get("peer")
        if peer is None:
            return await json_response({"error": "peer_required"}, status=400)
        if not node.is_leader():
            return leader_redirect(request)
        await node.add_peer(peer)
        return await json_response({"status": "ok", "peers": node.state.peers})

    app.router.add_get("/health", health)
    app.router.add_get("/kv/get", get_value)
    app.router.add_post("/kv/put", put_value)
    app.router.add_delete("/kv/delete", delete_value)
    app.router.add_post("/raft/request_vote", request_vote)
    app.router.add_post("/raft/append_entries", append_entries)
    app.router.add_post("/raft/join", join_cluster)
    return app


async def main() -> None:
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    node_id = os.getenv("NODE_ID", f"{host}:{port}")
    peers_raw = os.getenv("PEERS", "")
    peers = [peer.strip() for peer in peers_raw.split(",") if peer.strip()]
    data_dir = os.getenv("DATA_DIR", "/data")
    node = RaftNode(node_id=node_id, host=host, port=port, peers=peers, data_dir=data_dir)
    app = build_app(node)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await node.start()
    await site.start()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await node.stop()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
