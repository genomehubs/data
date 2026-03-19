"""
Orchestration flow for running tol-sdk flows in Docker with dependency isolation.

This lightweight Prefect flow delegates heavy lifting to Docker containers,
avoiding Pydantic compatibility issues while maintaining:
- Proper error reporting
- Prefect event emission for downstream flow triggering
- Retry logic at the orchestration level
- Integrated logging and monitoring
"""

import os
import subprocess
from typing import Optional

from prefect import flow, get_run_logger, task
from prefect.events import emit_event


@task(retries=2, retry_delay_seconds=60)
def run_docker_flow(
    flow_name: str,
    flow_script: str,
    output_path: str,
    s3_path: Optional[str] = None,
    min_records: Optional[int] = None,
    docker_image: str = "genomehubs-prefect-flows:latest",
    work_dir: str = "/app",
) -> dict:
    """
    Run a flow in Docker with SKIP_PREFECT to avoid Pydantic conflicts.

    Args:
        flow_name: Human-readable name for logging
        flow_script: Path to the flow Python script (relative to repo root)
        output_path: Output file path
        s3_path: Optional S3 path for output
        min_records: Optional minimum record count for validation
        docker_image: Docker image to use
        work_dir: Working directory in container

    Returns:
        dict: Result with status, output_path, and line_count

    Raises:
        RuntimeError: If Docker execution fails
    """
    logger = get_run_logger()

    logger.info(f"Starting {flow_name} in Docker...")
    logger.info(f"  Flow script: {flow_script}")
    logger.info(f"  Output path: {output_path}")
    if s3_path:
        logger.info(f"  S3 path: {s3_path}")
    if min_records:
        logger.info(f"  Min records: {min_records}")

    # Build Docker command
    # Note: NOT using -it (interactive/tty) because this runs from Prefect agent without a terminal
    # Get the repository root from current working directory
    # (When Prefect runs, cwd should be the cloned repository root)
    repo_root = os.getcwd()

    logger.info(f"Repository root: {repo_root}")
    logger.info(f"Repository root exists: {os.path.isdir(repo_root)}")

    # List key files to debug path issues
    flows_dir = os.path.join(repo_root, "flows")
    if os.path.isdir(flows_dir):
        logger.info("flows/ directory exists")
        updaters_dir = os.path.join(flows_dir, "updaters")
        if os.path.isdir(updaters_dir):
            logger.info("flows/updaters/ directory exists")
            flow_script_full = os.path.join(repo_root, flow_script)
            logger.info(f"Flow script location: {flow_script_full}")
            logger.info(f"Flow script exists: {os.path.isfile(flow_script_full)}")
        else:
            logger.warning("flows/updaters/ directory NOT found")
    else:
        logger.warning(f"flows/ directory NOT found in {repo_root}")

    # Log Docker info
    docker_version = subprocess.run(["docker", "--version"], capture_output=True, text=True, timeout=5)
    if docker_version.returncode == 0:
        logger.info(f"Docker: {docker_version.stdout.strip()}")

    # Build Docker command
    volume_mount = f"{repo_root}:{work_dir}"
    logger.info(f"Volume mount: {volume_mount}")

    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        volume_mount,
        "-e",
        "SKIP_PREFECT=true",
        "-e",
        f"PYTHONPATH={work_dir}",
        docker_image,
        "python",
        f"{work_dir}/{flow_script}",
        "--output_path",
        output_path,
    ]

    if s3_path:
        cmd.extend(["--s3_path", s3_path])

    if min_records is not None:
        cmd.extend(["--min_records", str(min_records)])

    logger.info(f"Docker command: {' '.join(cmd)}")

    # Execute Docker container
    try:
        logger.info("Executing Docker container...")
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
        )

        logger.info(f"Docker exit code: {result.returncode}")

        # Log output
        if result.stdout:
            logger.info(f"Docker STDOUT:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Docker STDERR:\n{result.stderr}")

        # Check for success
        if result.returncode != 0:
            raise RuntimeError(f"{flow_name} failed with exit code {result.returncode}: {result.stderr}")

        logger.info(f"✅ {flow_name} Docker execution completed")

        # Count output file lines if it exists
        line_count = 0
        logger.info(f"Checking output file: {output_path}")
        if os.path.exists(output_path):
            with open(output_path, "r") as f:
                line_count = sum(1 for _ in f)
            logger.info(f"Output file exists with {line_count} lines")

            # Validate minimum records if specified
            if min_records and line_count < min_records:
                raise RuntimeError(f"Output has only {line_count} lines, minimum {min_records} required")
        else:
            logger.warning(f"Output file does NOT exist at {output_path}")

        return {
            "status": "success",
            "output_path": output_path,
            "line_count": line_count,
            "s3_path": s3_path,
        }

    except subprocess.TimeoutExpired:
        raise RuntimeError(f"{flow_name} timed out after 1 hour")
    except Exception as e:
        logger.error(f"❌ {flow_name} failed: {str(e)}")
        raise


@task
def emit_completion_event(
    flow_name: str,
    result: dict,
    resource_type: str = "flow",
) -> None:
    """
    Emit a Prefect event for flow completion to trigger downstream flows.

    Args:
        flow_name: Name of the completed flow
        result: Result dictionary from run_docker_flow
        resource_type: Type of resource for Prefect event system
    """
    logger = get_run_logger()

    event_name = f"orchestrated.{flow_name}.completed"
    logger.info(f"Emitting event: {event_name}")

    emit_event(
        event=event_name,
        resource={
            "prefect.resource.id": f"orchestrated.{flow_name}",
            "prefect.resource.type": resource_type,
        },
        payload={
            "output_path": result.get("output_path"),
            "line_count": result.get("line_count"),
            "s3_path": result.get("s3_path"),
        },
    )


@flow(name="tol-genome-notes-orchestration")
def orchestrate_tol_genome_notes(
    output_path: str = "/tmp/tol_genome_notes.tsv",
    s3_path: Optional[str] = None,
    min_records: int = 1,
) -> dict:
    """
    Run the ToL genome notes update flow in Docker with proper orchestration.

    This flow:
    1. Runs the tol-genome-notes flow in a Docker container (SKIP_PREFECT=true)
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
        flow_name="tol-genome-notes",
        flow_script="flows/updaters/update_tol_genome_notes.py",
        output_path=output_path,
        s3_path=s3_path,
        min_records=min_records,
    )

    # Emit event for downstream triggering
    emit_completion_event(
        flow_name="tol-genome-notes",
        result=result,
        resource_type="tol.genome.notes",
    )

    logger.info(f"🎉 Orchestration complete: {result}")
    return result


@flow(name="full-tol-data-pipeline")
def full_tol_data_pipeline(
    tol_genome_notes_output: str = "/tmp/tol_genome_notes.tsv",
    tol_genome_notes_s3: Optional[str] = None,
) -> dict:
    """
    Example full pipeline orchestrating multiple tol-sdk flows.

    This demonstrates how to chain multiple flows with proper event emission
    for downstream triggering and error handling.

    Args:
        tol_genome_notes_output: Output path for tol genome notes
        tol_genome_notes_s3: Optional S3 path for tol genome notes

    Returns:
        dict: Combined results from all flows
    """
    logger = get_run_logger()

    logger.info("🚀 Starting full ToL data pipeline...")

    # Run first flow
    tol_notes_result = orchestrate_tol_genome_notes(
        output_path=tol_genome_notes_output,
        s3_path=tol_genome_notes_s3,
        min_records=1,
    )

    logger.info(f"✅ Phase 1 complete: {tol_notes_result}")

    # Additional flows can be chained here
    # Example:
    # other_result = orchestrate_other_flow(...)
    # if other_result['status'] != 'success':
    #     raise RuntimeError("Other flow failed")

    logger.info("🎉 Full pipeline completed successfully!")

    return {
        "tol_genome_notes": tol_notes_result,
        "status": "success",
    }


if __name__ == "__main__":
    # Example: Run directly for testing
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "full-pipeline":
        result = full_tol_data_pipeline()
    else:
        result = orchestrate_tol_genome_notes(
            output_path="/tmp/tol_genome_notes.tsv",
        )

    print("Result:", result)
