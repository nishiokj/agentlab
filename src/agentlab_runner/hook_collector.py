import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .harness_manifest import HarnessManifest
from .schemas import SchemaRegistry


@dataclass
class HookValidationResult:
    events: List[Dict[str, Any]]
    turn_count: int


class HookCollector:
    def __init__(self, schema_registry: SchemaRegistry) -> None:
        self.registry = schema_registry

    def _validate_header(self, header: Dict[str, Any], manifest: HarnessManifest) -> None:
        # Optional header must match manifest for overlapping keys.
        expected = {
            "hooks_schema_version": manifest.hooks_schema_version,
            "step_semantics": manifest.step_semantics,
            "integration_level": manifest.integration_level,
        }
        for key, value in expected.items():
            if key in header and header[key] != value:
                raise ValueError(f"Header mismatch for {key}")

    def collect(self, path: str, manifest: HarnessManifest) -> HookValidationResult:
        events: List[Dict[str, Any]] = []
        last_seq: Optional[int] = None
        seen_step = False
        current_step: Optional[int] = None
        pending_control_ack: Optional[int] = None
        stop_observed = False
        turn_count = 0

        with open(path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)

                if obj.get("event_type") == "hooks.header":
                    self._validate_header(obj, manifest)
                    continue

                # Validate schema per event
                self.registry.validate("hook_events_v1.jsonschema", obj)

                seq = obj.get("seq")
                if last_seq is not None and seq <= last_seq:
                    raise ValueError("Hook seq must be monotonically increasing")
                last_seq = seq

                event_type = obj.get("event_type")
                step_index = obj.get("step_index")

                if event_type == "agent_step_start":
                    seen_step = True
                    if pending_control_ack is not None:
                        raise ValueError("Missing control_ack before next step start")
                    if current_step is None:
                        current_step = step_index
                    else:
                        if step_index != current_step + 1:
                            raise ValueError("step_index must increment by 1")
                        current_step = step_index
                    if stop_observed:
                        raise ValueError("Stop observed but step continued")

                if event_type == "agent_step_end":
                    seen_step = True
                    if current_step is None or step_index != current_step:
                        raise ValueError("agent_step_end must match current step_index")
                    pending_control_ack = step_index

                if event_type == "control_ack":
                    if pending_control_ack is None or step_index != pending_control_ack:
                        raise ValueError("control_ack must match latest agent_step_end")
                    pending_control_ack = None
                    if obj.get("action_observed") == "stop":
                        stop_observed = True

                if event_type == "model_call_end":
                    turn_count += 1

                if seen_step and event_type in ("model_call_end", "tool_call_end", "error"):
                    if step_index is None:
                        raise ValueError("Causal events must include step_index when steps are used")

                events.append(obj)

        if pending_control_ack is not None:
            raise ValueError("Missing control_ack after agent_step_end")

        return HookValidationResult(events=events, turn_count=turn_count)
