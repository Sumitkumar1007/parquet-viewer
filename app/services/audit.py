from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class AuditLogger:
    def __init__(self, log_path: Path) -> None:
        self.log_path = log_path

    def log(self, action: str, username: str | None, ip_address: str | None, details: dict[str, Any]) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        event = {
            "timestamp": datetime.now(UTC).isoformat(),
            "action": action,
            "username": username,
            "ip_address": ip_address,
            "details": details,
        }
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
