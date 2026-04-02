"""
DEPRECATED: This module has been split into separate files in flows/orchestrators/

Please use:
    from flows.orchestrators import (
        orchestrate_tol_genome_notes,
        orchestrate_tol_portal_status,
        full_tol_data_pipeline,
        run_docker_flow,
        emit_completion_event,
    )

This module is kept for backwards compatibility but will be removed in a future release.
See flows/orchestrators/ for the new structure.
"""

# For backwards compatibility, re-export from new location
from flows.orchestrators import (
    emit_completion_event,
    full_tol_data_pipeline,
    orchestrate_tol_genome_notes,
    orchestrate_tol_portal_status,
    run_docker_flow,
)

__all__ = [
    "run_docker_flow",
    "emit_completion_event",
    "orchestrate_tol_genome_notes",
    "orchestrate_tol_portal_status",
    "full_tol_data_pipeline",
]


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
