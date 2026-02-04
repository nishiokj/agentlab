import json
import os
import subprocess
from typing import Any, Dict, List, Optional

from .schemas import SchemaRegistry


class HarnessExecutor:
    def __init__(self, schema_registry: SchemaRegistry) -> None:
        self.registry = schema_registry

    def run(
        self,
        command: List[str],
        trial_input: Dict[str, Any],
        input_path: str,
        output_path: str,
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(input_path, "w", encoding="utf-8") as f:
            json.dump(trial_input, f)

        self.registry.validate("trial_input_v1.jsonschema", trial_input)

        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            timeout=timeout,
            check=False,
        )

        if result.returncode != 0:
            raise RuntimeError(f"Harness exited non-zero: {result.returncode}")

        if not os.path.exists(output_path):
            raise FileNotFoundError("Harness did not write trial_output.json")

        with open(output_path, "r", encoding="utf-8") as f:
            output = json.load(f)

        self.registry.validate("trial_output_v1.jsonschema", output)
        return output
