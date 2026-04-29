from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.core.auth import hash_password


def main() -> None:
    parser = argparse.ArgumentParser(description="Create or update a user in runtime/users.json")
    parser.add_argument("username")
    parser.add_argument("password")
    parser.add_argument("--role", default="viewer")
    parser.add_argument("--users-file", default="runtime/users.json")
    args = parser.parse_args()

    users_file = Path(args.users_file)
    users_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {"users": []}
    if users_file.exists():
        payload = json.loads(users_file.read_text(encoding="utf-8"))

    found = False
    for item in payload.get("users", []):
        if item["username"] == args.username:
            item["password_hash"] = hash_password(args.password)
            item["role"] = args.role
            item["disabled"] = False
            found = True
            break

    if not found:
        payload.setdefault("users", []).append(
            {
                "username": args.username,
                "password_hash": hash_password(args.password),
                "role": args.role,
                "disabled": False,
            }
        )

    users_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Stored user {args.username} in {users_file.resolve()}")


if __name__ == "__main__":
    main()
