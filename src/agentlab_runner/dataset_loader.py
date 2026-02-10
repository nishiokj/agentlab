import json
from typing import Any, Dict, List, Optional


def load_jsonl(path: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    tasks: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            tasks.append(json.loads(line))
            if limit is not None and len(tasks) >= limit:
                break
    return tasks
