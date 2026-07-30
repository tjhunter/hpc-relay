import asyncio
import logging
import shlex
from pathlib import Path
from unittest.mock import AsyncMock

import hpc_relay.slurm as slurm
from hpc_relay.cmd_runners import CommandResult
from hpc_relay.cmd_runners._types import quote_path
from hpc_relay.result import OpError

from .fakes import FAKE_CONTEXT

_LOGGER = logging.getLogger(__name__)
_CTX = FAKE_CONTEXT


def test_quote_path_preserves_remote_home_and_quotes_untrusted_suffixes() -> None:
    cases = {
        "/tmp/plain": "/tmp/plain",
        "/tmp/a b": "'/tmp/a b'",
        "~": "~",
        "~/": "~/",
        "~/a b": "~/'a b'",
        "~alice/a b": "~alice/'a b'",
        "~odd~name/path": "'~odd~name/path'",
        "": "''",
    }

    for path, expected in cases.items():
        assert quote_path(path) == expected


def test_parse_job_id_accepts_scalar_and_array_ids() -> None:
    for stdout, expected in (
        ("Submitted batch job 12345\n", "12345"),
        ("banner text\nSubmitted batch job 12345_7\n", "12345_7"),
    ):
        result = CommandResult(stdout=stdout, stderr="", return_code=0)
        assert slurm._parse_job_id(result) == expected


def test_parse_job_id_rejects_nonpositive_and_malformed_ids() -> None:
    cases = (
        CommandResult(stdout="", stderr="", return_code=0),
        CommandResult(stdout="Submitted batch job 12", stderr="", return_code=1),
        CommandResult(stdout="Submitted batch job nope", stderr="", return_code=0),
        CommandResult(stdout="Submitted batch job 0", stderr="", return_code=0),
        CommandResult(stdout="Submitted batch job 123_", stderr="", return_code=0),
        CommandResult(stdout="Submitted batch job 123_bad", stderr="", return_code=0),
    )

    for result in cases:
        assert slurm._parse_job_id(result) is None


def test_terminal_state_classification_includes_timeout_and_cancellation() -> None:
    for state in ("COMPLETED", "FAILED", "TIMEOUT", "CANCELLED", "NODE_FAIL"):
        assert slurm.is_terminal_state(state)
    for state in ("PENDING", "RUNNING", "COMPLETING", "STAGE_OUT", "SUSPENDED"):
        assert not slurm.is_terminal_state(state)


def test_get_slurm_job_states_parses_noise_suffixes_and_input_order(monkeypatch) -> None:
    run_cmd = AsyncMock(
        return_value=CommandResult(
            stdout=(
                "ECMWF banner\n"
                "22|CANCELLED by 1234\n"
                "unrequested|FAILED\n"
                "11|RUNNING\n"
                "11.batch|COMPLETED\n"
            ),
            stderr="",
            return_code=0,
        )
    )
    monkeypatch.setattr(slurm, "run_cmd", run_cmd)

    result = asyncio.run(slurm.get_slurm_job_states(_CTX, ["11", "22"], _LOGGER))

    assert result == [
        slurm.SlurmJobInfo(job_id="11", state="RUNNING"),
        slurm.SlurmJobInfo(job_id="22", state="CANCELLED"),
    ]
    command_call = run_cmd.await_args
    assert command_call is not None
    command = command_call.args[1]
    assert command.command == [
        "sacct",
        "-j",
        "11,22",
        "-o",
        "JobID,State",
        "-X",
        "-n",
        "-P",
    ]


def test_get_slurm_job_states_rejects_unknown_state(monkeypatch) -> None:
    monkeypatch.setattr(
        slurm,
        "run_cmd",
        AsyncMock(return_value=CommandResult("11|MYSTERY\n", "", 0)),
    )

    result = asyncio.run(slurm.get_slurm_job_states(_CTX, ["11"], _LOGGER))

    assert isinstance(result, OpError)
    assert isinstance(result.err, ValueError)


def test_get_slurm_job_states_reports_missing_job(monkeypatch) -> None:
    monkeypatch.setattr(
        slurm,
        "run_cmd",
        AsyncMock(return_value=CommandResult("11|RUNNING\n", "", 0)),
    )

    result = asyncio.run(slurm.get_slurm_job_states(_CTX, ["11", "22"], _LOGGER))

    assert isinstance(result, OpError)
    assert isinstance(result.err, LookupError)
    assert "22" in str(result.err)


def test_get_slurm_job_states_wraps_command_failure(monkeypatch) -> None:
    connection_error = OpError(err=OSError("connection lost"))
    monkeypatch.setattr(slurm, "run_cmd", AsyncMock(return_value=connection_error))

    result = asyncio.run(slurm.get_slurm_job_states(_CTX, ["11"], _LOGGER))

    assert isinstance(result, OpError)
    assert isinstance(result.err, RuntimeError)
    assert "connection lost" in str(result.err)


def test_submit_slurm_builds_safe_command_and_resolves_output_paths(monkeypatch) -> None:
    run_cmd = AsyncMock(
        return_value=CommandResult("Submitted batch job 42\n", "", return_code=0)
    )
    monkeypatch.setattr(slurm, "run_cmd", run_cmd)
    monkeypatch.setattr(slurm, "slurm_account", lambda _ctx: "project")
    job = slurm.SlurmJob(
        job_name="name with spaces",
        command=["python", "-c", "print('hello world')"],
        working_directory="~/work dir",
        time_limit="00:10:00",
    )

    result = asyncio.run(slurm.submit_slurm(job, _CTX, _LOGGER))

    assert result == slurm.SlurmSubmissionResult(
        job_id="42",
        stdout=Path("~/work dir/slurm_job_name with spaces_42.out"),
        stderr=Path("~/work dir/slurm_job_name with spaces_42.out"),
    )
    command_call = run_cmd.await_args
    assert command_call is not None
    command = command_call.args[1]
    argv = shlex.split(command.command)
    assert argv[:2] == ["sbatch", "--job-name=name with spaces"]
    assert argv[argv.index("--output") + 1] == "~/work dir/slurm_job_name with spaces_%j.out"
    assert argv[argv.index("--chdir") + 1] == "~/work dir"
    assert "--time=00:10:00" in argv
    assert "--account=project" in argv
    assert isinstance(job.command, list)
    assert f"--wrap={shlex.join(job.command)}" in argv


def test_submit_slurm_validation_does_not_execute_command(monkeypatch) -> None:
    run_cmd = AsyncMock()
    monkeypatch.setattr(slurm, "run_cmd", run_cmd)
    job = slurm.SlurmJob(
        job_name="bad-path",
        command="true",
        working_directory="relative/path",
    )

    result = asyncio.run(slurm.submit_slurm(job, _CTX, _LOGGER))

    assert isinstance(result, OpError)
    assert isinstance(result.err, ValueError)
    run_cmd.assert_not_awaited()


def test_submit_slurm_preserves_ambiguous_success_output(monkeypatch) -> None:
    raw = CommandResult(stdout="submission accepted but id unavailable", stderr="", return_code=0)
    monkeypatch.setattr(slurm, "run_cmd", AsyncMock(return_value=raw))
    job = slurm.SlurmJob(job_name="ambiguous", command="true", stdout="/tmp/%j.out")

    result = asyncio.run(slurm.submit_slurm(job, _CTX, _LOGGER))

    assert isinstance(result, OpError)
    assert isinstance(result.err, slurm.SubmissionError)
    assert result.err.result is raw
    assert "id unavailable" in str(result.err)


def test_submit_slurm_propagates_connection_loss_without_resubmitting(monkeypatch) -> None:
    connection_loss = OpError(
        err=OSError("SSH connection closed after write"),
        stdout="",
        stderr="connection reset by peer",
        return_code=-1,
    )
    run_cmd = AsyncMock(return_value=connection_loss)
    monkeypatch.setattr(slurm, "run_cmd", run_cmd)
    job = slurm.SlurmJob(job_name="ambiguous", command="true", stdout="/tmp/%j.out")

    result = asyncio.run(slurm.submit_slurm(job, _CTX, _LOGGER))

    assert result is connection_loss
    run_cmd.assert_awaited_once()


def test_await_completion_caches_terminal_jobs_and_returns_timeout_and_cancelled(
    monkeypatch,
) -> None:
    get_states = AsyncMock(
        side_effect=[
            [
                slurm.SlurmJobInfo("1", "COMPLETED"),
                slurm.SlurmJobInfo("2", "RUNNING"),
                slurm.SlurmJobInfo("3", "PENDING"),
            ],
            [
                slurm.SlurmJobInfo("2", "TIMEOUT"),
                slurm.SlurmJobInfo("3", "CANCELLED"),
            ],
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(slurm, "get_slurm_job_states", get_states)
    monkeypatch.setattr(slurm.asyncio, "sleep", sleep)

    result = asyncio.run(slurm.await_completion(_CTX, ["1", "2", "3"], _LOGGER, 7))

    assert result == [
        slurm.SlurmJobInfo("1", "COMPLETED"),
        slurm.SlurmJobInfo("2", "TIMEOUT"),
        slurm.SlurmJobInfo("3", "CANCELLED"),
    ]
    assert get_states.await_args_list[0].args[1] == ["1", "2", "3"]
    assert get_states.await_args_list[1].args[1] == ["2", "3"]
    sleep.assert_awaited_once_with(7)


def test_await_completion_propagates_polling_connection_error(monkeypatch) -> None:
    connection_loss = OpError(err=OSError("sacct connection lost"))
    monkeypatch.setattr(
        slurm,
        "get_slurm_job_states",
        AsyncMock(return_value=connection_loss),
    )
    sleep = AsyncMock()
    monkeypatch.setattr(slurm.asyncio, "sleep", sleep)

    result = asyncio.run(slurm.await_completion(_CTX, ["1"], _LOGGER, 0))

    assert result is connection_loss
    sleep.assert_not_awaited()
