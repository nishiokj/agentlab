import argparse

from agentlab_cli import cli


class _RunResult:
    def __init__(self, run_id: str, report_dir: str) -> None:
        self.run_id = run_id
        self.report_dir = report_dir


class _FakeClient:
    def __init__(self) -> None:
        self.last_run = None

    def run(self, experiment, allow_missing_harness_manifest=False):
        self.last_run = (experiment, allow_missing_harness_manifest)
        return _RunResult("run_test", "/tmp/report")


def test_cmd_run_uses_sdk_client(monkeypatch):
    fake_client = _FakeClient()
    monkeypatch.setattr(cli, "_client", lambda: fake_client)

    args = argparse.Namespace(
        experiment="experiment.yaml",
        allow_missing_harness_manifest=True,
        open=False,
    )
    rc = cli.cmd_run(args)

    assert rc == 0
    assert fake_client.last_run == ("experiment.yaml", True)
