# (C) Copyright 2026 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

"""
A module for running command on different HPCs.

This module is independent from Prefect. It focuses on
abstracting away the details of communicating with the various HPCs,
with a focus on EuropHPC.
"""

from hpc_relay.cmd_runners._cineca import (
    CinecaCommandRunner,
    CinecaHpc,
    CinecaSshContext,
)
from hpc_relay.cmd_runners._cscs_firecrest import (
    CscsFirecrestCommandRunner,
    CscsFirecrestContext,
    CscsHpc,
)
from hpc_relay.cmd_runners._ecmwf_ecaccess import (
    EcmwfEcaccessCommandRunner,
    EcmwfEcaccessContext,
)
from hpc_relay.cmd_runners._exec_cmd import (
    CmdContext,
    get_command_runner,
    run_cmd,
    slurm_account,
)
from hpc_relay.cmd_runners._generic import GenericContext, GenericSshCommandRunner
from hpc_relay.cmd_runners._jsc import (
    JscHpc,
    JscUnicoreCommandRunner,
    JscUnicoreContext,
)
from hpc_relay.cmd_runners._local import LocalCommandRunner, LocalContext
from hpc_relay.cmd_runners._simple import SimpleSshCommandRunner, SimpleSshContext
from hpc_relay.cmd_runners._types import Command, CommandResult, CommandRunner

__all__ = [
    "Command",
    "CommandResult",
    "CommandRunner",
    "LocalCommandRunner",
    "LocalContext",
    "GenericSshCommandRunner",
    "GenericContext",
    "SimpleSshCommandRunner",
    "SimpleSshContext",
    "EcmwfEcaccessCommandRunner",
    "EcmwfEcaccessContext",
    "run_cmd",
    "CmdContext",
    "get_command_runner",
    "slurm_account",
    "CscsFirecrestCommandRunner",
    "CscsFirecrestContext",
    "CscsHpc",
    "CinecaCommandRunner",
    "CinecaSshContext",
    "CinecaHpc",
    "JscUnicoreCommandRunner",
    "JscUnicoreContext",
    "JscHpc",
]
