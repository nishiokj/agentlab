from dataclasses import dataclass
from typing import Optional


LEVEL_ORDER = ["cli_basic", "cli_events", "otel", "sdk_control", "sdk_full"]


@dataclass
class Evidence:
    hooks: bool = False
    traces: bool = False
    sdk_control: bool = False
    sdk_full: bool = False


def _rank(level: str) -> int:
    if level not in LEVEL_ORDER:
        return 0
    return LEVEL_ORDER.index(level)


def observed_level(evidence: Evidence) -> str:
    if evidence.sdk_full:
        return "sdk_full"
    if evidence.sdk_control:
        return "sdk_control"
    if evidence.traces:
        return "otel"
    if evidence.hooks:
        return "cli_events"
    return "cli_basic"


def derive_effective_level(manifest_level: str, evidence: Evidence) -> str:
    obs = observed_level(evidence)
    return LEVEL_ORDER[min(_rank(manifest_level), _rank(obs))]


def derive_replay_grade(effective_level: str, has_checkpoints: bool) -> str:
    if effective_level == "sdk_full":
        return "strict"
    if effective_level == "sdk_control":
        return "checkpointed" if has_checkpoints else "best_effort"
    if effective_level in ("otel", "cli_events"):
        return "best_effort"
    return "none"
