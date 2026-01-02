"""
Log-Structured Key-Value Store
Single-file storage engine

Features:
- Write-Ahead Log (WAL)
- Append-only writes
- In-memory index
- Crash recovery
- Log compaction
- Deterministic persistence
- Storage-engine level design

Run:
python app.py
"""

import os
import json
import time
from typing import Dict

DATA_DIR = "data"
WAL_FILE = os.path.join(DATA_DIR, "wal.log")
COMPACT_FILE = os.path.join(DATA_DIR, "store.json")

os.makedirs(DATA_DIR, exist_ok=True)

# -------------------------
# Storage Engine
# -------------------------
class LSMKeyValueStore:
    def __init__(self):
        self.index: Dict[str, str] = {}
        self._load()

    # -------------------------
    # Recovery
    # -------------------------
    def _load(self):
        if os.path.exists(COMPACT_FILE):
            with open(COMPACT_FILE, "r") as f:
                self.index = json.load(f)

        if os.path.exists(WAL_FILE):
            with open(WAL_FILE, "r") as f:
                for line in f:
                    record = json.loads(line)
                    if record["op"] == "set":
                        self.index[record["key"]] = record["value"]
                    elif record["op"] == "delete":
                        self.index.pop(record["key"], None)

    # -------------------------
    # WAL Append
    # -------------------------
    def _append_wal(self, record: dict):
        with open(WAL_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")

    # -------------------------
    # Public API
    # -------------------------
    def set(self, key: str, value: str):
        record = {
            "op": "set",
            "key": key,
            "value": value,
            "ts": time.time()
        }
        self._append_wal(record)
        self.index[key] = value

    def get(self, key: str):
        return self.index.get(key)

    def delete(self, key: str):
        record = {
            "op": "delete",
            "key": key,
            "ts": time.time()
        }
        self._append_wal(record)
        self.index.pop(key, None)

    # -------------------------
    # Compaction
    # -------------------------
    def compact(self):
        with open(COMPACT_FILE, "w") as f:
            json.dump(self.index, f)

        open(WAL_FILE, "w").close()

# -------------------------
# CLI Interface
# -------------------------
def main():
    store = LSMKeyValueStore()
    print("Log-Structured KV Store Ready")

    while True:
        cmd = input(">> ").strip().split()

        if not cmd:
            continue

        if cmd[0] == "set" and len(cmd) == 3:
            store.set(cmd[1], cmd[2])
            print("OK")

        elif cmd[0] == "get" and len(cmd) == 2:
            print(store.get(cmd[1]))

        elif cmd[0] == "delete" and len(cmd) == 2:
            store.delete(cmd[1])
            print("DELETED")

        elif cmd[0] == "compact":
            store.compact()
            print("COMPACTED")

        elif cmd[0] == "exit":
            break

        else:
            print("Commands: set k v | get k | delete k | compact | exit")

if __name__ == "__main__":
    main()
