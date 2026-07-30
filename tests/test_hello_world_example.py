# (C) Copyright 2026 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import logging
from contextlib import nullcontext
from importlib import import_module

import hpc_relay._sbatch as sbatch_module
import hpc_relay.prefect as prefect_module
from hpc_relay import flow, run, sbatch
from hpc_relay.cmd_runners import CommandResult

from .fakes import FAKE_CONTEXT, RecordingCommandRunner

run_module = import_module("hpc_relay.run")
slurm_module = import_module("hpc_relay.slurm")

# Unit-test context sentinel; the recording execution mock asserts this is used.
ctx = FAKE_CONTEXT


@flow(log_prints=True)
def hello_world(rerun_token=None):
    # The command to run on the HPC:
    command = "echo 'hello world'"

    # Run a command interactively:
    cmd_result = run(ctx, command=command)
    print(f"Command result: {cmd_result.stdout.strip()}")  # noqa: T201

    # Run a command on the HPC using sbatch:
    slurm_result = sbatch(
        ctx,
        job_name="hello_world_job",
        command=command,
        working_directory="~",
        time_limit="00:01:00",
        fetch_output=True,
    )
    print(f"Slurm job object: {slurm_result}")  # noqa: T201


def _ignore_rerun_info(_rerun_token: object) -> None:
    return None


async def _completed_job(*_args: object, **_kwargs: object) -> str:
    return "COMPLETED"


async def _ignore_async(*_args: object, **_kwargs: object) -> None:
    return None


def test_hello_world_flow_sends_expected_commands(monkeypatch) -> None:
    logger = logging.getLogger(__name__)
    runner = RecordingCommandRunner(
        responses=[
            CommandResult(stdout="hello world\n", stderr="", return_code=0),
            CommandResult(stdout="Submitted batch job 42\n", stderr="", return_code=0),
            CommandResult(
                stdout=(
                    '>>>> {"head_out":"hello world\\n","tail_out":"hello world\\n",'
                    '"head_err":null,"tail_err":null}\n'
                ),
                stderr="",
                return_code=0,
            ),
        ]
    )
    monkeypatch.setattr(prefect_module, "_log_rerun_info", _ignore_rerun_info)
    # Keep this as a unit test: use the real task functions, but bypass
    # Prefect's task engine and route all command execution to the recorder.
    monkeypatch.setitem(globals(), "run", run.fn)
    monkeypatch.setitem(globals(), "sbatch", sbatch.fn)
    monkeypatch.setattr(run_module, "run", run_module.run.fn)
    monkeypatch.setattr(
        sbatch_module,
        "_sbatch_submit_async",
        sbatch_module._sbatch_submit_async.fn,
    )
    monkeypatch.setattr(run_module, "get_run_logger", lambda: logger)
    monkeypatch.setattr(sbatch_module, "get_run_logger", lambda: logger)
    monkeypatch.setattr(run_module, "run_cmd", runner.run_cmd)
    monkeypatch.setattr(slurm_module, "run_cmd", runner.run_cmd)
    monkeypatch.setattr(sbatch_module, "get_command_runner", lambda _ctx: runner)
    monkeypatch.setattr(sbatch_module, "_set_status", _ignore_async)
    monkeypatch.setattr(sbatch_module, "wait_completion_single", _completed_job)
    monkeypatch.setattr(sbatch_module, "acreate_markdown_artifact", _ignore_async)
    monkeypatch.setattr(sbatch_module, "transaction", lambda **_kwargs: nullcontext())

    hello_world.fn(rerun_token=None)

    assert len(runner.calls) == 3
    interactive_cmd, sbatch_cmd, output_cmd = runner.calls
    assert interactive_cmd.command == "echo 'hello world'"
    assert interactive_cmd.working_directory is None

    assert isinstance(sbatch_cmd.command, str)
    assert sbatch_cmd.working_directory is None
    assert sbatch_cmd.command.startswith("sbatch --job-name=hello_world_job ")
    assert "--chdir ~" in sbatch_cmd.command
    assert "--time=00:01:00" in sbatch_cmd.command
    assert "--wrap='echo '\"'\"'hello world'\"'\"''" in sbatch_cmd.command

    assert isinstance(output_cmd.command, str)
    assert "slurm_job_hello_world_job_42.out" in output_cmd.command
