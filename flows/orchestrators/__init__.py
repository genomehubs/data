"""Orchestrators for running flows in Docker with dependency isolation.

This package contains Prefect flows that orchestrate heavy workloads by running
them in Docker containers with SKIP_PREFECT=true to avoid library version conflicts
(particularly Pydantic conflicts between Prefect and tol-sdk).

Flows:
    - orchestrate_tol_genome_notes: ToL genome notes update orchestration
    - orchestrate_tol_portal_status: ToL portal status update orchestration
    - full_tol_data_pipeline: Example pipeline combining multiple orchestrated flows

Tasks:
    - run_docker_flow: Generic Docker execution task
    - emit_completion_event: Prefect event emission for downstream flow triggers
"""

from flows.orchestrators.tasks import emit_completion_event, run_docker_flow
from flows.orchestrators.tol_data_pipeline import full_tol_data_pipeline
from flows.orchestrators.tol_genome_notes_orchestration import orchestrate_tol_genome_notes
from flows.orchestrators.tol_portal_status_orchestration import orchestrate_tol_portal_status

__all__ = [
    "run_docker_flow",
    "emit_completion_event",
    "orchestrate_tol_genome_notes",
    "orchestrate_tol_portal_status",
    "full_tol_data_pipeline",
]
