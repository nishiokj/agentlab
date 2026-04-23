#!/usr/bin/env python3
"""
Validate, repair, and append .lab/journal.jsonl entries for experiment_journal_entry_v1.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - non-Unix fallback
    fcntl = None


SCHEMA_VERSION = "experiment_journal_entry_v1"
ATTEMPT_STATUSES = {"preflight_failed", "run_failed", "run_killed", "run_completed"}
VERDICTS = {"confirmed", "refuted", "inconclusive"}
REQUIRED_FIELDS = {
    "schema_version",
    "experiment_id",
    "package_digest",
    "timestamp",
    "hypothesis",
    "attempt_status",
    "verdict",
}
ALLOWED_FIELDS = REQUIRED_FIELDS | {
    "run_id",
    "parent_run_id",
    "changes",
    "blocking_checks",
    "pass_rate",
    "baseline_pass_rate",
    "effect",
    "regression_count",
    "regressions",
    "novel_pass_count",
    "novel_passes",
    "insight",
    "next_steps",
}


@dataclass
class JournalFailure:
    line_no: int
    kind: str
    message: str
    is_last_nonempty: bool


@dataclass
class JournalLoad:
    entries: list[dict[str, Any]]
    valid_prefix_bytes: bytes
    failure: JournalFailure | None
    entry_count: int


class ValidationError(ValueError):
    pass


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def ensure_string(value: Any, field: str) -> None:
    if not isinstance(value, str):
        raise ValidationError(f"field '{field}' must be a string")


def ensure_nullable_string(value: Any, field: str) -> None:
    if value is not None and not isinstance(value, str):
        raise ValidationError(f"field '{field}' must be a string or null")


def ensure_string_array(value: Any, field: str) -> None:
    if not isinstance(value, list):
        raise ValidationError(f"field '{field}' must be an array")
    for idx, item in enumerate(value):
        if not isinstance(item, str):
            raise ValidationError(f"field '{field}[{idx}]' must be a string")


def ensure_iso_datetime(value: Any, field: str) -> None:
    ensure_string(value, field)
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValidationError(f"field '{field}' must be an ISO 8601 datetime") from exc


def ensure_rate(value: Any, field: str) -> None:
    if not is_number(value):
        raise ValidationError(f"field '{field}' must be a number")
    if value < 0 or value > 1:
        raise ValidationError(f"field '{field}' must be between 0 and 1")


def ensure_nonnegative_int(value: Any, field: str) -> None:
    if not is_integer(value):
        raise ValidationError(f"field '{field}' must be an integer")
    if value < 0:
        raise ValidationError(f"field '{field}' must be >= 0")


def validate_changes(value: Any) -> None:
    if not isinstance(value, list):
        raise ValidationError("field 'changes' must be an array")
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValidationError(f"field 'changes[{idx}]' must be an object")
        unexpected = sorted(set(item) - {"knob", "from", "to"})
        if unexpected:
            joined = ", ".join(unexpected)
            raise ValidationError(f"field 'changes[{idx}]' has unexpected keys: {joined}")
        for required in ("knob", "from", "to"):
            if required not in item:
                raise ValidationError(f"field 'changes[{idx}]' is missing '{required}'")
        ensure_string(item["knob"], f"changes[{idx}].knob")


def validate_entry(entry: Any) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise ValidationError("entry must be a JSON object")

    unexpected = sorted(set(entry) - ALLOWED_FIELDS)
    if unexpected:
        joined = ", ".join(unexpected)
        raise ValidationError(f"unexpected field(s): {joined}")

    missing = sorted(field for field in REQUIRED_FIELDS if field not in entry)
    if missing:
        joined = ", ".join(missing)
        raise ValidationError(f"missing required field(s): {joined}")

    if entry.get("schema_version") != SCHEMA_VERSION:
        raise ValidationError(
            f"field 'schema_version' must equal '{SCHEMA_VERSION}'"
        )

    ensure_string(entry["experiment_id"], "experiment_id")
    ensure_string(entry["package_digest"], "package_digest")
    ensure_iso_datetime(entry["timestamp"], "timestamp")
    ensure_string(entry["hypothesis"], "hypothesis")

    if entry["attempt_status"] not in ATTEMPT_STATUSES:
        raise ValidationError(
            "field 'attempt_status' must be one of: "
            + ", ".join(sorted(ATTEMPT_STATUSES))
        )

    if entry["verdict"] not in VERDICTS:
        raise ValidationError(
            "field 'verdict' must be one of: " + ", ".join(sorted(VERDICTS))
        )

    if "run_id" in entry:
        ensure_nullable_string(entry["run_id"], "run_id")
    if "parent_run_id" in entry:
        ensure_nullable_string(entry["parent_run_id"], "parent_run_id")
    if "changes" in entry:
        validate_changes(entry["changes"])
    if "blocking_checks" in entry:
        ensure_string_array(entry["blocking_checks"], "blocking_checks")
    if "pass_rate" in entry:
        ensure_rate(entry["pass_rate"], "pass_rate")
    if "baseline_pass_rate" in entry:
        ensure_rate(entry["baseline_pass_rate"], "baseline_pass_rate")
    if "effect" in entry:
        ensure_string(entry["effect"], "effect")
    if "regression_count" in entry:
        ensure_nonnegative_int(entry["regression_count"], "regression_count")
    if "regressions" in entry:
        ensure_string_array(entry["regressions"], "regressions")
    if "novel_pass_count" in entry:
        ensure_nonnegative_int(entry["novel_pass_count"], "novel_pass_count")
    if "novel_passes" in entry:
        ensure_string_array(entry["novel_passes"], "novel_passes")
    if "insight" in entry:
        ensure_string(entry["insight"], "insight")
    if "next_steps" in entry:
        ensure_string_array(entry["next_steps"], "next_steps")

    return entry


def compact_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def utc_timestamp_slug() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def load_journal_bytes(raw: bytes) -> JournalLoad:
    lines = raw.splitlines(keepends=True)
    nonempty_line_numbers = [
        idx + 1 for idx, line in enumerate(lines) if line.strip()
    ]
    last_nonempty = nonempty_line_numbers[-1] if nonempty_line_numbers else None

    entries: list[dict[str, Any]] = []
    valid_prefix = bytearray()

    for idx, raw_line in enumerate(lines, start=1):
        if not raw_line.strip():
            valid_prefix.extend(raw_line)
            continue
        try:
            text_line = raw_line.decode("utf-8")
        except UnicodeDecodeError as exc:
            return JournalLoad(
                entries=entries,
                valid_prefix_bytes=bytes(valid_prefix),
                failure=JournalFailure(
                    line_no=idx,
                    kind="decode",
                    message=str(exc),
                    is_last_nonempty=(idx == last_nonempty),
                ),
                entry_count=len(entries),
            )
        try:
            entry = json.loads(text_line)
        except json.JSONDecodeError as exc:
            return JournalLoad(
                entries=entries,
                valid_prefix_bytes=bytes(valid_prefix),
                failure=JournalFailure(
                    line_no=idx,
                    kind="parse",
                    message=str(exc),
                    is_last_nonempty=(idx == last_nonempty),
                ),
                entry_count=len(entries),
            )
        try:
            validate_entry(entry)
        except ValidationError as exc:
            return JournalLoad(
                entries=entries,
                valid_prefix_bytes=bytes(valid_prefix),
                failure=JournalFailure(
                    line_no=idx,
                    kind="schema",
                    message=str(exc),
                    is_last_nonempty=(idx == last_nonempty),
                ),
                entry_count=len(entries),
            )
        entries.append(entry)
        valid_prefix.extend(raw_line)

    return JournalLoad(
        entries=entries,
        valid_prefix_bytes=bytes(valid_prefix),
        failure=None,
        entry_count=len(entries),
    )


def load_journal(path: Path) -> JournalLoad:
    if not path.exists():
        return JournalLoad(entries=[], valid_prefix_bytes=b"", failure=None, entry_count=0)
    return load_journal_bytes(path.read_bytes())


def duplicate_of(existing: dict[str, Any], new_entry: dict[str, Any]) -> bool:
    existing_run_id = existing.get("run_id")
    new_run_id = new_entry.get("run_id")
    if existing_run_id is not None and new_run_id is not None:
        return existing_run_id == new_run_id
    return (
        existing.get("package_digest") == new_entry.get("package_digest")
        and existing.get("attempt_status") == new_entry.get("attempt_status")
    )


def lock_file(handle: Any) -> None:
    if fcntl is not None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)


def fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def emit(payload: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, sort_keys=True))
        return
    if payload.get("ok"):
        print(payload.get("message", "ok"))
        return
    print(payload.get("message", "error"), file=sys.stderr)


def command_check(args: argparse.Namespace) -> int:
    result = load_journal(args.journal)
    if result.failure is None:
        emit(
            {
                "ok": True,
                "journal": str(args.journal),
                "entry_count": result.entry_count,
                "message": f"journal is valid ({result.entry_count} entries)",
            },
            args.json,
        )
        return 0
    failure = result.failure
    emit(
        {
            "ok": False,
            "journal": str(args.journal),
            "entry_count": result.entry_count,
            "line": failure.line_no,
            "kind": failure.kind,
            "is_last_nonempty": failure.is_last_nonempty,
            "message": (
                f"journal validation failed at line {failure.line_no} "
                f"({failure.kind}): {failure.message}"
            ),
        },
        args.json,
    )
    return 1


def command_repair(args: argparse.Namespace) -> int:
    if not args.journal.exists():
        emit(
            {
                "ok": True,
                "journal": str(args.journal),
                "message": "journal does not exist; nothing to repair",
            },
            args.json,
        )
        return 0

    raw = args.journal.read_bytes()
    result = load_journal_bytes(raw)
    if result.failure is None:
        emit(
            {
                "ok": True,
                "journal": str(args.journal),
                "entry_count": result.entry_count,
                "message": "journal is already valid; no repair needed",
            },
            args.json,
        )
        return 0

    failure = result.failure
    if failure.kind != "parse" or not failure.is_last_nonempty:
        emit(
            {
                "ok": False,
                "journal": str(args.journal),
                "line": failure.line_no,
                "kind": failure.kind,
                "message": (
                    "repair only supports a truncated final JSON line; "
                    f"found {failure.kind} failure at line {failure.line_no}"
                ),
            },
            args.json,
        )
        return 1

    backup = args.journal.with_name(
        f"{args.journal.stem}.corrupt.{utc_timestamp_slug()}{args.journal.suffix}"
    )
    shutil.copy2(args.journal, backup)
    args.journal.write_bytes(result.valid_prefix_bytes)
    fsync_directory(args.journal.parent)
    emit(
        {
            "ok": True,
            "journal": str(args.journal),
            "backup": str(backup),
            "entry_count": result.entry_count,
            "message": (
                f"repaired journal by dropping malformed final line; backup at {backup}"
            ),
        },
        args.json,
    )
    return 0


def command_append(args: argparse.Namespace) -> int:
    try:
        with args.entry_file.open("r", encoding="utf-8") as handle:
            entry = json.load(handle)
    except OSError as exc:
        emit(
            {
                "ok": False,
                "entry_file": str(args.entry_file),
                "message": f"failed to read entry file: {exc}",
            },
            args.json,
        )
        return 1
    except json.JSONDecodeError as exc:
        emit(
            {
                "ok": False,
                "entry_file": str(args.entry_file),
                "message": f"entry file is not valid JSON: {exc}",
            },
            args.json,
        )
        return 1
    try:
        validate_entry(entry)
    except ValidationError as exc:
        emit(
            {
                "ok": False,
                "entry_file": str(args.entry_file),
                "message": f"entry validation failed: {exc}",
            },
            args.json,
        )
        return 1

    args.journal.parent.mkdir(parents=True, exist_ok=True)
    with args.journal.open("a+b") as handle:
        lock_file(handle)
        handle.seek(0)
        raw = handle.read()
        result = load_journal_bytes(raw)
        if result.failure is not None:
            failure = result.failure
            emit(
                {
                    "ok": False,
                    "journal": str(args.journal),
                    "line": failure.line_no,
                    "kind": failure.kind,
                    "message": (
                        f"journal validation failed before append at line {failure.line_no} "
                        f"({failure.kind}): {failure.message}"
                    ),
                },
                args.json,
            )
            return 1

        for existing in result.entries:
            if duplicate_of(existing, entry):
                emit(
                    {
                        "ok": True,
                        "journal": str(args.journal),
                        "duplicate": True,
                        "entry_count": result.entry_count,
                        "message": "duplicate entry detected; nothing appended",
                    },
                    args.json,
                )
                return 0

        encoded = compact_json(entry)
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        if size > 0:
            handle.seek(-1, os.SEEK_END)
            if handle.read(1) != b"\n":
                handle.seek(0, os.SEEK_END)
                handle.write(b"\n")
        handle.seek(0, os.SEEK_END)
        handle.write(encoded + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    fsync_directory(args.journal.parent)
    emit(
        {
            "ok": True,
            "journal": str(args.journal),
            "duplicate": False,
            "entry_count": result.entry_count + 1,
            "message": "entry appended",
        },
        args.json,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage .lab/journal.jsonl for experiment_journal_entry_v1."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="Validate an experiment journal.")
    check.add_argument("--journal", type=Path, required=True)
    check.add_argument("--json", action="store_true")
    check.set_defaults(func=command_check)

    repair = subparsers.add_parser(
        "repair", help="Repair a journal with a truncated final JSON line."
    )
    repair.add_argument("--journal", type=Path, required=True)
    repair.add_argument("--json", action="store_true")
    repair.set_defaults(func=command_repair)

    append = subparsers.add_parser(
        "append", help="Append a validated entry to the journal."
    )
    append.add_argument("--journal", type=Path, required=True)
    append.add_argument("--entry-file", type=Path, required=True)
    append.add_argument("--json", action="store_true")
    append.set_defaults(func=command_append)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
