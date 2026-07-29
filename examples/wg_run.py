#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#     "weathergen-prefect-dags",
# ]
#
# [tool.uv.sources]
# # Directly pull the package from Github.
# # weathergen-prefect-dags = { git = "https://github.com/ecmwf/WeatherGenerator", branch = "tjh/dev/prefect-test", subdirectory = "packages/prefect-dags" }
#
# # When developing locally, swap the source above for the line below:
# weathergen-prefect-dags = { path = "../", editable = true }
# ///
from weathergen.launch_slurm import launch_slurm, wait_for_completion

from hpc_relay import flow, is_err, run
from hpc_relay.cmd_runners import SimpleSshContext

# The run context defines where the commands will be executed.
ctx = SimpleSshContext(
    host="hpc-login",
)


@flow(log_prints=True)
def weathergen(
    rerun_token=None, pub_branch: str = "origin/develop", pri_branch: str = "origin/main"
):
    # The main pipeline on weathergen.
    # The command to run on the HPC:
    _ = run(
        ctx,
        f"""
        cd ~/work/WeatherGenerator
        git remote update
        git checkout {pub_branch}
        cd ~/work/WeatherGenerator-private
        git remote update
        git checkout {pri_branch}
        """,
    )

    jobs = launch_slurm(
        ctx,
        "./work/WeatherGenerator-private",
        working_dir="~",
        stage="train",
        time="1-00:00:00",
        base_config="./work/WeatherGenerator/config/default_config.yml",
    )
    print(jobs)
    assert not is_err(jobs)
    final_status = wait_for_completion(ctx, jobs)
    print(final_status)


if __name__ == "__main__":
    weathergen(rerun_token=None)
