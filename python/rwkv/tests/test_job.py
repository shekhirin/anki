# Copyright: Ankitects Pty Ltd and contributors
# License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

import json

import pytest
from rwkv import job


def test_cron_defaults_to_preview_and_sync(monkeypatch, tmp_path):
    calls = []
    result = {"state": "succeeded", "applied_cards": 0, "report": {"dry_run": True}}

    def request(_url, path, payload=None):
        calls.append((path, payload))
        return {"job_id": "job-1"} if payload is not None else result

    output = tmp_path / "result.json"
    monkeypatch.setattr(job, "request", request)
    monkeypatch.setattr("sys.argv", ["rwkv.job", "--output", str(output)])
    job.main()
    assert calls[0] == (
        "/v1/rwkv/run",
        {
            "query": "",
            "retention": 0.9,
            "maximum_interval": 3650,
            "apply": False,
            "sync": True,
        },
    )
    assert json.loads(output.read_text()) == result
    assert list(tmp_path.iterdir()) == [output]


def test_cron_failure_saves_applied_count_and_never_resubmits(monkeypatch, tmp_path):
    calls = []
    result = {
        "state": "failed",
        "applied_cards": 2,
        "error": "upload failed",
        "report": {},
    }

    def request(_url, path, payload=None):
        calls.append((path, payload))
        return {"job_id": "job-2"} if payload is not None else result

    output = tmp_path / "result.json"
    monkeypatch.setattr(job, "request", request)
    monkeypatch.setattr("sys.argv", ["rwkv.job", "--apply", "--output", str(output)])
    with pytest.raises(SystemExit) as error:
        job.main()
    assert error.value.code == 1
    assert calls[0][1]["apply"] is True
    assert len([payload for _, payload in calls if payload is not None]) == 1
    assert json.loads(output.read_text())["applied_cards"] == 2


def test_resuming_job_does_not_start_another(monkeypatch):
    paths = []

    def request(_url, path, payload=None):
        assert payload is None
        paths.append(path)
        return {"state": "succeeded", "applied_cards": 0}

    monkeypatch.setattr(job, "request", request)
    monkeypatch.setattr("sys.argv", ["rwkv.job", "--job-id", "already-accepted"])
    job.main()
    assert paths == ["/v1/rwkv/jobs/already-accepted"]


def test_timeout_leaves_server_job_running(monkeypatch):
    monkeypatch.setattr(job, "request", lambda *_: {"state": "running"})
    times = iter([0, 10])
    monkeypatch.setattr(job.time, "monotonic", lambda: next(times))
    with pytest.raises(TimeoutError, match="does not cancel"):
        job.wait("http://localhost", "job-3", 1)


def test_lost_admission_response_is_not_retried(monkeypatch):
    calls = []

    def request(*args):
        calls.append(args)
        raise TimeoutError("lost response")

    monkeypatch.setattr(job, "request", request)
    monkeypatch.setattr("sys.argv", ["rwkv.job", "--apply"])
    with pytest.raises(TimeoutError):
        job.main()
    assert len(calls) == 1
