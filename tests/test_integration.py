from agentlab_runner import Evidence, derive_effective_level, derive_replay_grade


def test_integration_level_derivation():
    evidence = Evidence(hooks=True)
    level = derive_effective_level("sdk_full", evidence)
    assert level == "cli_events"


def test_replay_grade_mapping():
    assert derive_replay_grade("cli_basic", False) == "none"
    assert derive_replay_grade("cli_events", False) == "best_effort"
    assert derive_replay_grade("sdk_control", False) == "best_effort"
    assert derive_replay_grade("sdk_control", True) == "checkpointed"
    assert derive_replay_grade("sdk_full", True) == "strict"
