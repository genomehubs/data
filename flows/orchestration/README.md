# Orchestration Flows

This directory contains Prefect orchestration flows that manage flows with dependency conflicts (like tol-sdk) by running them in isolated Docker containers.

## Overview

The orchestration pattern solves the pydantic compatibility issue with tol-sdk by:

1. **Running lightweight Prefect flows** in your normal environment (no tol-sdk imports)
2. **Delegating heavy lifting** to Docker containers with complete dependency isolation
3. **Maintaining Prefect capabilities** like event emission, error handling, and monitoring

## Files

### `tol_data_orchestration.py`

Main orchestration module with reusable components:

- **`run_docker_flow()`** - Generic task to run any flow in Docker
  - Handles Docker subprocess execution
  - Validates exit codes
  - Counts output records
  - Raises exceptions on failure

- **`emit_completion_event()`** - Emit Prefect events for downstream triggering
  - Sends completion events with payload
  - Enables dependency-based flow triggering

- **`orchestrate_tol_genome_notes()`** - Specific flow for ToL genome notes
  - Example orchestration flow
  - Runs tol-genome-notes in Docker
  - Emits completion events
  - Can be deployed as a Prefect deployment

- **`full_tol_data_pipeline()`** - Example pipeline with multiple flows
  - Demonstrates chaining multiple flows
  - Shows error handling between flows
  - Template for more complex pipelines

## Usage

### Direct Python Execution

For testing without Prefect:

```bash
python flows/orchestration/tol_data_orchestration.py
```

With custom parameters:

```python
from flows.orchestration.tol_data_orchestration import orchestrate_tol_genome_notes

result = orchestrate_tol_genome_notes(
    output_path="/tmp/output.tsv",
    s3_path="s3://bucket/path/file.tsv",
    min_records=100
)
print(result)  # {'status': 'success', 'output_path': ..., 'line_count': ...}
```

### Prefect Deployment

Deploy as a Prefect deployment (see `prefect.yaml.docker-example`):

```yaml
deployments:
  - name: tol-genome-notes-orchestration
    entrypoint: flows/orchestration/tol_data_orchestration.py:orchestrate_tol_genome_notes
    parameters:
      output_path: "/tmp/tol_genome_notes.tsv"
      s3_path: "s3://goat/resources/tol_genome_notes.tsv"
    work_pool:
      name: goat-data
```

Then trigger via CLI:

```bash
prefect deployment run tol-genome-notes-orchestration
```

Or schedule it in Prefect UI.

### Direct Flow Call

From another Prefect flow:

```python
from prefect import flow
from flows.orchestration.tol_data_orchestration import orchestrate_tol_genome_notes

@flow
def my_pipeline():
    result = orchestrate_tol_genome_notes(
        output_path="/tmp/output.tsv"
    )
    if result['status'] == 'success':
        # Trigger next flow
        pass
```

## Extending for Other Flows

To add orchestration for a new flow:

```python
from flows.orchestration.tol_data_orchestration import run_docker_flow, emit_completion_event
from prefect import flow

@flow(name="my-flow-orchestration")
def orchestrate_my_flow(
    output_path: str,
    s3_path: Optional[str] = None,
):
    """Orchestrate my-flow in Docker."""

    result = run_docker_flow(
        flow_name="my-flow",
        flow_script="flows/updaters/my_flow.py",
        output_path=output_path,
        s3_path=s3_path,
        min_records=100,
    )

    emit_completion_event(
        flow_name="my-flow",
        result=result,
        resource_type="my.resource.type",
    )

    return result
```

## Error Handling

The orchestration flows handle:

1. **Docker execution failures** - Raised as RuntimeError with full stderr
2. **Missing output files** - Logged but not fatal (line_count = 0)
3. **Insufficient records** - Raised as RuntimeError if line count < min_records
4. **Timeouts** - Timeout after 1 hour with RuntimeError
5. **Subprocess errors** - Full traceback logged via Prefect logger

Example error handling:

```python
from flows.orchestration.tol_data_orchestration import orchestrate_tol_genome_notes
from prefect import flow

@flow
def my_pipeline():
    try:
        result = orchestrate_tol_genome_notes()
    except RuntimeError as e:
        # Handle flow failure
        print(f"Flow failed: {e}")
        # Retry, send alert, skip next steps, etc.
        raise
```

## Environment Variables

The orchestration flows use:

- **`SKIP_PREFECT=true`** - Required in Docker container to avoid Pydantic issues
- **`PREFECT_*`** - Standard Prefect environment variables (apikey, etc.)

## Benefits

✅ **No Pydantic conflicts** - Prefect runs light logic, Docker handles heavy lifting  
✅ **Full event system** - Emits completion events for downstream triggering  
✅ **Proper error handling** - Validates exit codes and output  
✅ **Prefect monitoring** - Flows appear in Prefect UI with logging  
✅ **Reusable pattern** - Apply to any flow with dependency conflicts  
✅ **Dependency isolation** - Docker ensures exact dependency versions

## Limitations

⚠️ **No task-level retries** - Retries happen at orchestration level only  
⚠️ **No caching** - Docker containers run fresh each time  
⚠️ **Subprocess overhead** - Slight latency from Docker startup  
⚠️ **Event timing** - Events emit after Docker execution completes

## Related Files

- `Dockerfile` - Docker image with tol-sdk and dependencies
- `requirements-docker.txt` - Pinned versions for Docker
- `docker-compose.yml` - Local testing configuration
- `prefect.yaml.docker-example` - Example Prefect deployment config
- `DOCKER_SETUP.md` - Complete Docker setup guide
