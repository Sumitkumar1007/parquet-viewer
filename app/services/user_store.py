from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from app.core.auth import hash_password, verify_password


@dataclass
class UserRecord:
    username: str
    password_hash: str
    role: str = "viewer"
    disabled: bool = False


class UserStore:
    def __init__(self, users_file: Path, admin_username: str, admin_password_hash: str) -> None:
        self.users_file = users_file
        self.admin_username = admin_username
        self.admin_password_hash = admin_password_hash

    def authenticate(self, username: str, password: str) -> UserRecord | None:
        user = self.get_user(username)
        if not user or user.disabled:
            return None
        if not verify_password(password, user.password_hash):
            return None
        return user

    def get_user(self, username: str) -> UserRecord | None:
        for user in self._load_users():
            if user.username == username:
                return user
        return None

    def change_password(self, username: str, current_password: str, new_password: str) -> None:
        user = self.authenticate(username, current_password)
        if not user:
            raise ValueError("Current password is invalid.")
        users = self._load_users()
        for item in users:
            if item.username == username:
                item.password_hash = hash_password(new_password)
        self._save_users(users)

    def _default_admin(self) -> list[UserRecord]:
        if not self.admin_username or not self.admin_password_hash:
            return []
        return [
            UserRecord(
                username=self.admin_username,
                password_hash=self.admin_password_hash,
                role="admin",
                disabled=False,
            )
        ]

    def _load_users(self) -> list[UserRecord]:
        if not self.users_file.exists():
            return self._default_admin()
        raw = json.loads(self.users_file.read_text(encoding="utf-8"))
        users = []
        for item in raw.get("users", []):
            users.append(
                UserRecord(
                    username=item["username"],
                    password_hash=item["password_hash"],
                    role=item.get("role", "viewer"),
                    disabled=bool(item.get("disabled", False)),
                )
            )
        return users or self._default_admin()

    def _save_users(self, users: list[UserRecord]) -> None:
        self.users_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "users": [
                {
                    "username": user.username,
                    "password_hash": user.password_hash,
                    "role": user.role,
                    "disabled": user.disabled,
                }
                for user in users
            ]
        }
        self.users_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
