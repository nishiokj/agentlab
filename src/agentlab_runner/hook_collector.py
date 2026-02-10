import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .harness_manifest import HarnessManifest
from .schemas import SchemaRegistry


@dataclass
class HookValidationResult:
    events: List[Dict[str, Any]]
    turn_count: int


class HookValidationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        line_num: Optional[int] = None,
        seq: Optional[int] = None,
        event_type: Optional[str] = None,
        raw_line: Optional[str] = None,
        details: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.line_num = line_num
        self.seq = seq
        self.event_type = event_type
        self.raw_line = raw_line
        self.details = details


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
                raw_line = line.rstrip("\n")
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception as e:
                    raise HookValidationError(
                        "Invalid JSON in hook stream",
                        line_num=line_num,
                        raw_line=raw_line,
                        details=str(e),
                    ) from e

                if obj.get("event_type") == "hooks.header":
                    try:
                        self._validate_header(obj, manifest)
                    except Exception as e:
                        raise HookValidationError(
                            "Header does not match harness_manifest.json",
                            line_num=line_num,
                            seq=obj.get("seq"),
                            event_type="hooks.header",
                            raw_line=raw_line,
                            details=str(e),
                        ) from e
                    continue

                # Validate schema per event
                try:
                    self.registry.validate("hook_events_v1.jsonschema", obj)
                except Exception as e:
                    raise HookValidationError(
                        "Schema validation failed for hook event",
                        line_num=line_num,
                        seq=obj.get("seq"),
                        event_type=obj.get("event_type"),
                        raw_line=raw_line,
                        details=str(e),
                    ) from e

                seq = obj.get("seq")
                if last_seq is not None and seq <= last_seq:
                    raise HookValidationError(
                        "Hook seq must be monotonically increasing",
                        line_num=line_num,
                        seq=seq,
                        event_type=obj.get("event_type"),
                        raw_line=raw_line,
                        details=f"previous_seq={last_seq}, current_seq={seq}",
                    )
                last_seq = seq

                event_type = obj.get("event_type")
                step_index = obj.get("step_index")

                if event_type == "agent_step_start":
                    seen_step = True
                    if pending_control_ack is not None:
                        raise HookValidationError(
                            "Missing control_ack before next step start",
                            line_num=line_num,
                            seq=seq,
                            event_type=event_type,
                            raw_line=raw_line,
                            details=f"pending_control_ack_for_step={pending_control_ack}",
                        )
                    if current_step is None:
                        current_step = step_index
                    else:
                        if step_index != current_step + 1:
                            raise HookValidationError(
                                "step_index must increment by 1",
                                line_num=line_num,
                                seq=seq,
                                event_type=event_type,
                                raw_line=raw_line,
                                details=f"expected={current_step + 1}, got={step_index}",
                            )
                        current_step = step_index
                    if stop_observed:
                        raise HookValidationError(
                            "Stop observed but step continued",
                            line_num=line_num,
                            seq=seq,
                            event_type=event_type,
                            raw_line=raw_line,
                        )

                if event_type == "agent_step_end":
                    seen_step = True
                    if current_step is None or step_index != current_step:
                        raise HookValidationError(
                            "agent_step_end must match current step_index",
                            line_num=line_num,
                            seq=seq,
                            event_type=event_type,
                            raw_line=raw_line,
                            details=f"current_step={current_step}, got={step_index}",
                        )
                    pending_control_ack = step_index

                if event_type == "control_ack":
                    if pending_control_ack is None or step_index != pending_control_ack:
                        raise HookValidationError(
                            "control_ack must match latest agent_step_end",
                            line_num=line_num,
                            seq=seq,
                            event_type=event_type,
                            raw_line=raw_line,
                            details=f"expected_step={pending_control_ack}, got={step_index}",
                        )
                    pending_control_ack = None
                    if obj.get("action_observed") == "stop":
                        stop_observed = True

                if event_type == "model_call_end":
                    turn_count += 1

                if seen_step and event_type in ("model_call_end", "tool_call_end", "error"):
                    if step_index is None:
                        raise HookValidationError(
                            "Causal events must include step_index when steps are used",
                            line_num=line_num,
                            seq=seq,
                            event_type=event_type,
                            raw_line=raw_line,
                        )

                events.append(obj)

        if pending_control_ack is not None:
            raise HookValidationError(
                "Missing control_ack after agent_step_end",
                details=f"pending_control_ack_for_step={pending_control_ack}",
            )

        return HookValidationResult(events=events, turn_count=turn_count)
