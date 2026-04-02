"""Full ToL data pipeline orchestration."""

from typing import Optional

from prefect import flow, get_run_logger

from flows.orchestrators.tol_genome_notes_orchestration import orchestrate_tol_genome_notes


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
    result = full_tol_data_pipeline()
    print("Result:", result)
