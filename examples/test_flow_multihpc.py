#!/usr/bin/env -S uv run --script
# (C) Copyright 2026 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#     "hpc-relay",
# ]
#
# [tool.uv.sources]
# # Directly pull the package from Github.
# # hpc-relay = { git = "https://github.com/tjhunter/hpc-relay.git" }
#
# # When developing locally, swap the source above for the line below:
# hpc-relay = { path = "../", editable = true }
# ///
from hpc_relay import SlurmJobResult, flow, run, sbatch, task
from hpc_relay.cmd_runners import (
    CscsFirecrestContext,
    # EcmwfEcaccessContext,
    JscUnicoreContext,
    SimpleSshContext,
)

all_contexts = {
    "atos-ssh": SimpleSshContext(
        host="hpc-login",
    ),
    # "atos-ecaccess": EcmwfEcaccessContext(
    #     cert_path="~/.ecaccess_cert.crt",
    # ),
    "santis-firecrest": CscsFirecrestContext(
        hpc="santis",
        account="ch17",
        consumer_key_path="~/.ssh/cscs_consumer_key",
        consumer_secret_path="~/.ssh/cscs_consumer_secret",
    ),
    "santis-ssh": SimpleSshContext(
        host="santis",
        account="ch17",
    ),
    "jupiter-unicore": JscUnicoreContext(hpc="jupiter", project="weatherai"),
    "jupiter-ssh": SimpleSshContext(host="jupiter"),
}


@task(task_run_name="get_pwd-{ctx_name}")
def get_pwd(ctx_name) -> str:
    ctx = all_contexts[ctx_name]
    # ECMWF appends many other lines to the output, so we need to get the last one:
    res = run(ctx, command=["pwd"])
    assert res.stdout, "No output from pwd command"
    last_line = res.stdout.strip().split("\n")[-1]
    print(f"last line of pwd output: '{last_line}'")
    return last_line


@task(task_run_name="sleep_and_print-{ctx_name}-{sleep_sec}s")
def sleep_and_print(ctx_name, sleep_sec: int, pwd: str) -> SlurmJobResult:
    ctx = all_contexts[ctx_name]
    print(f"Working directory is {pwd}, sleeping for {sleep_sec} seconds...")
    res = sbatch(
        ctx,
        job_name="test_job",
        command=[
            "python3",
            "-c",
            f"import time; time.sleep({sleep_sec}); print('hello')",
        ],
        time_limit="00:01:00",
        working_directory=pwd,
    )
    print(f"result: {res}, type: {type(res)}")
    return res


@flow(log_prints=True)
def run_multi_hpc(
    rerun_token=None,
):
    jobs = []
    for ctx_name in all_contexts.keys():
        print(f"Running flow with context: {ctx_name}")
        # Get pwd on HPC
        pwd = get_pwd.submit(ctx_name)
        sleep_times = [5, 10]
        # Submit all my jobs
        jobs.extend([sleep_and_print.submit(ctx_name, sleep_sec, pwd) for sleep_sec in sleep_times])
    # Wait for all the jobs to complete and print the results:
    for job in jobs:
        res = job.result()
        print(f"Job result: {res}, ")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--rerun-token", default=None)
    args = parser.parse_args()
    run_multi_hpc(rerun_token=args.rerun_token)
