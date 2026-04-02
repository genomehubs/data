"""Orchestration flow for ToL portal status updates."""

from typing import Optional

from prefect import flow, get_run_logger

from flows.orchestrators.tasks import emit_completion_event, run_docker_flow


@flow(name="tol-portal-status-orchestration")
def orchestrate_tol_portal_status(
    output_path: str = "/tmp/tol_portal_status.tsv",
    s3_path: Optional[str] = None,
    min_records: int = 1,
) -> dict:
    """
    Run the ToL portal status update flow in Docker with proper orchestration.

    This flow:
    1. Runs the tol-portal-status flow in a Docker container (SKIP_PREFECT=true)
    2. Handles errors and retries at the orchestration level
    3. Emits Prefect events for downstream flow triggering
    4. Reports success/failure through Prefect's monitoring system

    Args:
        output_path: Path to save the output TSV file
        s3_path: Optional S3 path to upload results
        min_records: Minimum number of records required for success

    Returns:
        dict: Result with status, output_path, and line_count
    """
    logger = get_run_logger()

    # Run the flow in Docker
    result = run_docker_flow(
        flow_name="tol-portal-status",
        flow_script="flows/updaters/update_tol_portal_status.py",
        output_path=output_path,
        s3_path=s3_path,
        min_records=min_records,
    )

    # Emit event for downstream triggering
    emit_completion_event(
        flow_name="tol-portal-status",
        result=result,
        resource_type="tol.portal.status",
    )

    logger.info(f"🎉 Orchestration complete: {result}")
    return result


if __name__ == "__main__":
    result = orchestrate_tol_portal_status(
        output_path="/tmp/tol_portal_status.tsv",
    )
    print("Result:", result)
