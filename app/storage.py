import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class LogEntry:
    term: int
    command: Dict[str, Any]


@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)
    peers: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [{"term": entry.term, "command": entry.command} for entry in self.log],
            "peers": self.peers,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "PersistentState":
        log_entries = [LogEntry(term=item["term"], command=item["command"]) for item in payload.get("log", [])]
        return cls(
            current_term=payload.get("current_term", 0),
            voted_for=payload.get("voted_for"),
            log=log_entries,
            peers=payload.get("peers", []),
        )


class StateStorage:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self.state_file = os.path.join(data_dir, "state.json")
        os.makedirs(self.data_dir, exist_ok=True)

    def load(self) -> PersistentState:
        if not os.path.exists(self.state_file):
            return PersistentState()
        with open(self.state_file, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return PersistentState.from_dict(payload)

    def save(self, state: PersistentState) -> None:
        tmp_file = f"{self.state_file}.tmp"
        with open(tmp_file, "w", encoding="utf-8") as handle:
            json.dump(state.to_dict(), handle, ensure_ascii=False, indent=2)
        os.replace(tmp_file, self.state_file)
