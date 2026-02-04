import uuid


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def new_run_id() -> str:
    return _new_id("run")


def new_trial_id() -> str:
    return _new_id("trial")


def new_variant_id() -> str:
    return _new_id("variant")


def new_task_id() -> str:
    return _new_id("task")
