import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import ClientSession

from app.storage import LogEntry, PersistentState, StateStorage


@dataclass
class AppendEntriesResult:
    term: int
    success: bool


@dataclass
class RequestVoteResult:
    term: int
    vote_granted: bool


class RaftNode:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peers: List[str],
        data_dir: str,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.storage = StateStorage(data_dir)
        self.state = self.storage.load()
        if peers:
            self.state.peers = list(sorted(set(self.state.peers).union(peers)))
        self.kv_store: Dict[str, Any] = {}
        self.commit_index = -1
        self.last_applied = -1
        self.role = "follower"
        self.leader_id: Optional[str] = None
        self.election_reset_event = asyncio.Event()
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.election_task: Optional[asyncio.Task] = None
        self.client_session = ClientSession()
        self.apply_log()

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def peers(self) -> List[str]:
        return [peer for peer in self.state.peers if peer != self.address]

    def apply_log(self) -> None:
        self.kv_store = {}
        for idx, entry in enumerate(self.state.log):
            self.apply_entry(entry)
            self.last_applied = idx
        self.commit_index = self.last_applied

    def apply_entry(self, entry: LogEntry) -> None:
        command = entry.command
        op = command.get("op")
        key = command.get("key")
        if op == "PUT":
            self.kv_store[key] = command.get("value")
        elif op == "DELETE":
            self.kv_store.pop(key, None)

    async def start(self) -> None:
        self.election_task = asyncio.create_task(self.run_election_timer())

    async def stop(self) -> None:
        if self.election_task:
            self.election_task.cancel()
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        await self.client_session.close()

    def persist_state(self) -> None:
        self.storage.save(self.state)

    def election_timeout(self) -> float:
        return random.uniform(2.0, 3.5)

    def heartbeat_interval(self) -> float:
        return 0.7

    async def run_election_timer(self) -> None:
        while True:
            self.election_reset_event.clear()
            timeout = self.election_timeout()
            try:
                await asyncio.wait_for(self.election_reset_event.wait(), timeout=timeout)
                continue
            except asyncio.TimeoutError:
                await self.start_election()

    def reset_election_timer(self) -> None:
        self.election_reset_event.set()

    async def start_election(self) -> None:
        self.role = "candidate"
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        self.persist_state()
        votes = 1
        total_nodes = len(self.peers) + 1
        last_log_index, last_log_term = self.last_log_info()
        tasks = [
            self.request_vote(peer, self.state.current_term, last_log_index, last_log_term)
            for peer in self.peers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, RequestVoteResult):
                if result.term > self.state.current_term:
                    self.state.current_term = result.term
                    self.state.voted_for = None
                    self.role = "follower"
                    self.persist_state()
                    return
                if result.vote_granted:
                    votes += 1
        if votes > total_nodes // 2:
            await self.become_leader()
        else:
            self.role = "follower"

    async def become_leader(self) -> None:
        self.role = "leader"
        self.leader_id = self.node_id
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        self.heartbeat_task = asyncio.create_task(self.run_heartbeat())

    async def run_heartbeat(self) -> None:
        while True:
            await self.broadcast_append_entries(full_sync=True)
            await asyncio.sleep(self.heartbeat_interval())

    def last_log_info(self) -> Tuple[int, int]:
        if not self.state.log:
            return -1, 0
        last_index = len(self.state.log) - 1
        return last_index, self.state.log[last_index].term

    async def request_vote(
        self,
        peer: str,
        term: int,
        last_log_index: int,
        last_log_term: int,
    ) -> RequestVoteResult:
        url = f"http://{peer}/raft/request_vote"
        payload = {
            "term": term,
            "candidate_id": self.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        }
        try:
            async with self.client_session.post(url, json=payload, timeout=1.5) as resp:
                data = await resp.json()
                return RequestVoteResult(term=data["term"], vote_granted=data["vote_granted"])
        except Exception:
            return RequestVoteResult(term=term, vote_granted=False)

    async def append_entries(
        self,
        peer: str,
        term: int,
        entries: List[LogEntry],
        commit_index: int,
        leader_id: str,
        full_sync: bool,
    ) -> AppendEntriesResult:
        url = f"http://{peer}/raft/append_entries"
        payload = {
            "term": term,
            "leader_id": leader_id,
            "prev_log_index": -1 if full_sync else len(self.state.log) - len(entries) - 1,
            "prev_log_term": 0,
            "entries": [
                {"term": entry.term, "command": entry.command}
                for entry in (self.state.log if full_sync else entries)
            ],
            "leader_commit": commit_index,
            "full_sync": full_sync,
        }
        try:
            async with self.client_session.post(url, json=payload, timeout=1.5) as resp:
                data = await resp.json()
                return AppendEntriesResult(term=data["term"], success=data["success"])
        except Exception:
            return AppendEntriesResult(term=term, success=False)

    async def broadcast_append_entries(self, full_sync: bool = False) -> bool:
        entries = []
        if not full_sync and self.state.log:
            entries = [self.state.log[-1]]
        tasks = [
            self.append_entries(
                peer,
                self.state.current_term,
                entries,
                self.commit_index,
                self.node_id,
                full_sync=full_sync,
            )
            for peer in self.peers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successes = 1
        for result in results:
            if isinstance(result, AppendEntriesResult):
                if result.term > self.state.current_term:
                    self.state.current_term = result.term
                    self.state.voted_for = None
                    self.role = "follower"
                    self.persist_state()
                    return False
                if result.success:
                    successes += 1
        return successes > (len(self.peers) + 1) // 2

    async def handle_append_entries(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        term = payload["term"]
        if term < self.state.current_term:
            return {"term": self.state.current_term, "success": False}
        self.state.current_term = term
        self.state.voted_for = None
        self.role = "follower"
        self.leader_id = payload.get("leader_id")
        self.reset_election_timer()
        if payload.get("full_sync"):
            self.state.log = [
                LogEntry(term=entry["term"], command=entry["command"])
                for entry in payload.get("entries", [])
            ]
            self.storage.persist_log(self.state.log)
        else:
            prev_index = payload.get("prev_log_index", -1)
            if prev_index >= 0 and prev_index >= len(self.state.log):
                return {"term": self.state.current_term, "success": False}
            self.state.log = self.state.log[: prev_index + 1]
            for entry in payload.get("entries", []):
                self.state.log.append(LogEntry(term=entry["term"], command=entry["command"]))
            self.storage.persist_log(self.state.log)
        leader_commit = payload.get("leader_commit", -1)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.state.log) - 1)
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                self.apply_entry(self.state.log[self.last_applied])
        self.persist_state()
        return {"term": self.state.current_term, "success": True}

    async def handle_request_vote(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        term = payload["term"]
        if term < self.state.current_term:
            return {"term": self.state.current_term, "vote_granted": False}
        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None
            self.role = "follower"
        last_log_index, last_log_term = self.last_log_info()
        up_to_date = (
            payload["last_log_term"] > last_log_term
            or (
                payload["last_log_term"] == last_log_term
                and payload["last_log_index"] >= last_log_index
            )
        )
        if (self.state.voted_for in (None, payload["candidate_id"])) and up_to_date:
            self.state.voted_for = payload["candidate_id"]
            self.persist_state()
            self.reset_election_timer()
            return {"term": self.state.current_term, "vote_granted": True}
        self.persist_state()
        return {"term": self.state.current_term, "vote_granted": False}

    async def replicate_command(self, command: Dict[str, Any]) -> bool:
        entry = LogEntry(term=self.state.current_term, command=command)
        self.state.log.append(entry)
        self.storage.append_wal(entry)
        self.persist_state()
        success = await self.broadcast_append_entries(full_sync=False)
        if success:
            self.commit_index = len(self.state.log) - 1
            self.last_applied = self.commit_index
            self.apply_entry(entry)
        return success

    def get_value(self, key: str) -> Any:
        return self.kv_store.get(key)

    def delete_value(self, key: str) -> Any:
        return self.kv_store.pop(key, None)

    def is_leader(self) -> bool:
        return self.role == "leader"

    def leader_address(self) -> Optional[str]:
        return self.leader_id

    async def add_peer(self, peer: str) -> None:
        if peer not in self.state.peers:
            self.state.peers.append(peer)
            self.persist_state()
            await self.broadcast_append_entries(full_sync=True)

    async def close(self) -> None:
        await self.client_session.close()
