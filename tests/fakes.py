from dataclasses import dataclass, field
from logging import Logger
from typing import cast

from hpc_relay.cmd_runners import CmdContext, Command, CommandResult
from hpc_relay.result import Result


@dataclass(frozen=True)
class FakeContext:
    """Sentinel context that the production command dispatcher cannot execute."""


FAKE_CONTEXT = cast(CmdContext, FakeContext())


@dataclass
class RecordingCommandRunner:
    """Scripted runner that records commands without starting a process."""

    hpc: str = "cluster"
    name: str = "recording"
    responses: list[Result[CommandResult]] = field(default_factory=list)
    calls: list[Command] = field(default_factory=list)

    def run(self, cmd: Command, logger: Logger) -> Result[CommandResult]:
        self.calls.append(cmd)
        if not self.responses:
            raise AssertionError(f"Unexpected command with no scripted response: {cmd}")
        return self.responses.pop(0)

    async def run_cmd(
        self,
        ctx: CmdContext,
        cmd: Command,
        logger: Logger,
    ) -> Result[CommandResult]:
        if ctx is not FAKE_CONTEXT:
            raise AssertionError(f"Unexpected context: {ctx!r}")
        return self.run(cmd, logger)
