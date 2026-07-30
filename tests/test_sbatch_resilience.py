# (C) Copyright 2026 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call
from uuid import UUID, uuid4

import httpx
from prefect.client.schemas.objects import StateType

import hpc_relay._sbatch as sbatch_module
from hpc_relay.cmd_runners import CommandResult
from hpc_relay.result import OpError

from .fakes import FAKE_CONTEXT, RecordingCommandRunner

_LOGGER = logging.getLogger(__name__)
_CTX = FAKE_CONTEXT
_RUNNER = RecordingCommandRunner()


class _CancelledFlowClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    async def read_flow_run(self, flow_run_id: UUID):
        return SimpleNamespace(state_type=StateType.CANCELLED)


def _configure_cancelled_flow(monkeypatch, run_result):
    flow_run_id = uuid4()
    monkeypatch.setattr(sbatch_module.prefect.runtime.flow_run, "id", flow_run_id)
    monkeypatch.setattr(sbatch_module, "get_client", lambda: _CancelledFlowClient())
    payloads = [
        sbatch_module._SlurmJobPrefectStatus(
            job_id="owned-1",
            hpc="cluster",
            status="RUNNING",
            flow_run_id=str(flow_run_id),
        ),
        sbatch_module._SlurmJobPrefectStatus(
            job_id="other-flow",
            hpc="cluster",
            status="RUNNING",
            flow_run_id=str(uuid4()),
        ),
    ]
    monkeypatch.setattr(
        sbatch_module,
        "_list_status_payloads",
        AsyncMock(return_value=payloads),
    )
    set_status = AsyncMock()
    runner = RecordingCommandRunner(responses=[run_result])
    monkeypatch.setattr(sbatch_module, "_set_status", set_status)
    monkeypatch.setattr(sbatch_module, "run_cmd", runner.run_cmd)
    return set_status, runner


def test_wait_completion_single_bounds_prefect_api_outage(monkeypatch) -> None:
    error = httpx.ConnectError("prefect unavailable")
    poll = AsyncMock(side_effect=[error, error])
    start = datetime(2026, 1, 1, tzinfo=UTC)
    now = Mock(side_effect=[start, start + timedelta(seconds=301)])
    sleep = AsyncMock()
    monkeypatch.setattr(sbatch_module, "_poll_completion_once", poll)
    monkeypatch.setattr(sbatch_module, "_now_utc", now)
    monkeypatch.setattr(sbatch_module.asyncio, "sleep", sleep)

    result = asyncio.run(
        sbatch_module.wait_completion_single(_LOGGER, _CTX, "42", _RUNNER)
    )

    assert isinstance(result, OpError)
    assert result.err is error
    sleep.assert_awaited_once_with(5)
    assert poll.await_count == 2


def test_wait_completion_single_retries_with_exponential_backoff(monkeypatch) -> None:
    first_error = OSError("temporary outage")
    second_error = OSError("still unavailable")
    poll = AsyncMock(side_effect=[first_error, second_error, "COMPLETED"])
    start = datetime(2026, 1, 1, tzinfo=UTC)
    now = Mock(side_effect=[start, start + timedelta(seconds=10)])
    sleep = AsyncMock()
    monkeypatch.setattr(sbatch_module, "_poll_completion_once", poll)
    monkeypatch.setattr(sbatch_module, "_now_utc", now)
    monkeypatch.setattr(sbatch_module.asyncio, "sleep", sleep)

    result = asyncio.run(
        sbatch_module.wait_completion_single(_LOGGER, _CTX, "42", _RUNNER)
    )

    assert result == "COMPLETED"
    assert sleep.await_args_list == [call(5), call(10)]


def test_poll_completion_once_returns_timeout_and_cleans_status(monkeypatch) -> None:
    monkeypatch.setattr(
        sbatch_module,
        "_flow_cancellation_guard",
        AsyncMock(return_value=sbatch_module.CancelledSlurmJobs(cancelled=[])),
    )
    monkeypatch.setattr(sbatch_module, "_try_update_status", AsyncMock())
    monkeypatch.setattr(sbatch_module, "_read_status", AsyncMock(return_value="TIMEOUT"))
    delete_status = AsyncMock()
    monkeypatch.setattr(sbatch_module, "_delete_status", delete_status)

    result = asyncio.run(
        sbatch_module._poll_completion_once(_LOGGER, _CTX, "42", _RUNNER)
    )

    assert result == "TIMEOUT"
    delete_status.assert_awaited_once_with("cluster", "42")


def test_cancellation_guard_marks_and_cancels_only_flow_owned_jobs(monkeypatch) -> None:
    set_status, runner = _configure_cancelled_flow(
        monkeypatch,
        CommandResult(stdout="", stderr="", return_code=0),
    )

    result = asyncio.run(
        sbatch_module._flow_cancellation_guard(_LOGGER, _CTX, runner)
    )

    assert result == sbatch_module.CancelledSlurmJobs(cancelled=["owned-1"])
    set_status.assert_awaited_once_with("cluster", "owned-1", "CANCELLING")
    assert [command.command for command in runner.calls] == [["scancel", "owned-1"]]


def test_cancellation_guard_preserves_scancel_failure_details(monkeypatch) -> None:
    set_status, runner = _configure_cancelled_flow(
        monkeypatch,
        CommandResult(stdout="partial", stderr="permission denied", return_code=1),
    )

    result = asyncio.run(
        sbatch_module._flow_cancellation_guard(_LOGGER, _CTX, runner)
    )

    assert isinstance(result, OpError)
    assert isinstance(result.err, RuntimeError)
    assert result.stdout == "partial"
    assert result.stderr == "permission denied"
    assert result.return_code == 1
    set_status.assert_awaited_once_with("cluster", "owned-1", "CANCELLING")


def test_cancellation_guard_propagates_connection_loss(monkeypatch) -> None:
    connection_loss = OpError(err=OSError("connection reset by peer"), return_code=-1)
    set_status, runner = _configure_cancelled_flow(monkeypatch, connection_loss)

    result = asyncio.run(
        sbatch_module._flow_cancellation_guard(_LOGGER, _CTX, runner)
    )

    assert result is connection_loss
    set_status.assert_awaited_once_with("cluster", "owned-1", "CANCELLING")
