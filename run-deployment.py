from __future__ import annotations

from prefect.deployments import run_deployment

run_deployment(
    name="end-of-run-workflow/ucal-end-of-run-workflow-docker",
    # name="end-of-run-workflow/end_of_run_workflow_deployment",
    parameters={"stop_doc": {"run_start": ""}},
    timeout=15,  # don't wait for the run to finish # edit to 15 sec
)
