# Docker Setup for Prefect ToL Genome Notes Flow

## Problem

The `update_tol_genome_notes.py` flow uses `tol-sdk 1.9.4`, which has dependency conflicts with newer versions of `click` and `pydantic`:

```
tol-sdk 1.9.4 requires Click==8.1.7, but you have click 8.3.1
tol-sdk 1.9.4 requires pydantic==2.11.4, but you have pydantic 2.12.5
```

## Solution

Use Docker containers to isolate your Prefect flows with compatible dependencies.

## Quick Start

### 1. Build the Docker Image

```bash
docker build -t genomehubs-prefect-flows:latest .
```

### 2. Run the Flow in Docker

**Option A: Using docker-compose (easiest, includes SKIP_PREFECT)**

```bash
docker-compose up
```

**Option B: Direct docker run with SKIP_PREFECT**

```bash
# Run with SKIP_PREFECT=true to avoid Pydantic compatibility issues
docker run -it --rm \
  -v $(pwd):/app \
  -e SKIP_PREFECT=true \
  genomehubs-prefect-flows:latest \
  python flows/updaters/update_tol_genome_notes.py \
  --output-path /tmp/tol_genome_notes.tsv
```

### 3. Run Specific Flows

````bash
# Run update_tol_genome_notes
docker run -it --rm \
  -v $(pwd):/app \
  -e SKIP_PREFECT=true \
  genomehubs-prefect-flows:latest \
  python flows/updaters/update_tol_genome_notes.py \
| `tol-sdk` | 1.9.4 | Fixed |
| `click` | 8.1.7 | Required by tol-sdk (strict) |
| `pydantic` | 2.11.4 | Required by tol-sdk 1.9.4 |
| `prefect` | 3.0.11 | Compatible with pydantic 2.11.4 (initial Pydantic v2 release) |
| `python` | 3.10, 3.11, 3.12 | Works with tol-sdk |

### Why Prefect 3.0.11?

- **tol-sdk 1.9.4** requires `pydantic==2.11.4` (strict)
- **Prefect 3.1.12+** requires `pydantic>=2.12.0` due to schema generation changes
- **Prefect 3.0.11** is the latest version in the 3.0.x line and is compatible with pydantic 2.11.4
- Prefect 3.0.x still supports all modern features needed for your flows

### Your Current Configuration

The `requirements-docker.txt` file pins all compatible versions for this setup:
- Prefect 3.0.11
- tol-sdk 1.9.4
- click 8.1.7
- pydantic 2.11.4

## Using SKIP_PREFECT Environment Variable

Your codebase has a built-in mechanism (`conditional_import.py`) to skip loading Prefect entirely when needed. This is recommended for running flows in Docker with tol-sdk to avoid Pydantic compatibility issues.

When `SKIP_PREFECT=true`:
- The `@flow` and `@task` decorators become no-ops (they don't change behavior)
- The flow logic executes normally as a Python script
- No Prefect machinery is loaded (avoiding Pydantic schema issues)
- This is a safe, supported approach already in your codebase

**Recommended for Docker**: Always use `SKIP_PREFECT=true` when running with tol-sdk in Docker.

## Architecture Options

There are three approaches to integrate tol-sdk flows with Prefect:

### Option 1: Direct Docker Execution (Simplest)
Run the flow directly in Docker skipping Prefect entirely.

```bash
docker run -it --rm -v $(pwd):/app -e SKIP_PREFECT=true \
  genomehubs-prefect-flows:latest \
  python flows/updaters/update_tol_genome_notes.py \
  --output-path /tmp/output.tsv
````

**Pros:** Simple, no Prefect overhead  
**Cons:** No event-based triggering, can't track in Prefect UI, manual orchestration

### Option 2: Docker Work Pool (Native but Complex)

Deploy flows to run in Docker work pools via Prefect.

```yaml
deployments:
  - name: update-tol-genome-notes
    entrypoint: flows/updaters/update_tol_genome_notes.py:update_tol_genome_notes
    work_pool:
      name: docker-pool
```

**Pros:** Native Prefect integration  
**Cons:** Requires Docker work pool setup, may have Pydantic issues, more complex

### Option 3: Orchestration Layer (Recommended) ⭐

Lightweight Prefect orchestration that delegates to Docker containers.

```yaml
deployments:
  - name: tol-genome-notes-orchestration
    entrypoint: flows/orchestration/tol_data_orchestration.py:orchestrate_tol_genome_notes
    work_pool:
      name: goat-data # Regular work pool
```

**Pros:**

- Prefect runs lightweight orchestration (no Pydantic issues)
- Docker containers get complete dependency isolation
- Full event emission for downstream triggering
- Proper error reporting and retry logic
- Can chain multiple flows
- Works with your existing Prefect infrastructure

**Cons:** Slight extra complexity (subprocess call)

## Recommended: Orchestration Flow Approach

We've created [flows/orchestration/tol_data_orchestration.py](flows/orchestration/tol_data_orchestration.py) which demonstrates this pattern:

```python
from flows.orchestration.tol_data_orchestration import orchestrate_tol_genome_notes

# This runs:
# 1. Orchestration in Prefect (lightweight, no imports of tol-sdk)
# 2. Heavy lifting in Docker (SKIP_PREFECT=true, isolated deps)
# 3. Emits Prefect events for downstream flow triggering
result = orchestrate_tol_genome_notes(
    output_path="/tmp/tol_genome_notes.tsv",
    s3_path="s3://goat/resources/tol_genome_notes.tsv"
)
```

### Key Features of the Orchestration Flow:

- **No Pydantic Conflicts**: Prefect only processes lightweight logic
- **Event Emission**: Emits `orchestrated.tol-genome-notes.completed` event
- **Downstream Triggering**: Other flows can be triggered on event completion
- **Proper Error Handling**: Subprocess exit codes are validated
- **Logging**: Full logging through Prefect logger
- **Retry Logic**: Retries at orchestration level if needed
- **Monitoring**: Results appear in Prefect UI

### To Use the Orchestration Flow:

1. Ensure Docker image is built:

   ```bash
   docker build -t genomehubs-prefect-flows:latest .
   ```

2. Deploy the orchestration flow:

   ```bash
   prefect deploy -f prefect.yaml.docker-example
   ```

3. Trigger the deployment:

   ```bash
   prefect deployment run tol-genome-notes-orchestration
   ```

4. View in Prefect UI - the flow will appear with proper logging and status

## Setting Up Prefect Deployments with Docker

### Update prefect.yaml for Docker Execution

You can configure your Prefect deployments to run in Docker containers:

```yaml
deployments:
  - name: update-tol-genome-notes
    entrypoint: flows/updaters/update_tol_genome_notes.py:update_tol_genome_notes
    parameters:
      output_path: "/home/ubuntu/tmp/tol_genome_notes.tsv"
      s3_path: "s3://goat/resources/tol_genome_notes.tsv"
    # Use a Docker work pool for this deployment
    work_pool:
      name: docker-pool
      work_queue_name: default
```

### Create a Docker Work Pool

```bash
# Using prefect CLI
docker run -it --rm \
  -v $(pwd):/app \
  genomehubs-prefect-flows:latest \
  prefect work-pool create --type docker docker-pool
```

## Troubleshooting

### Check Versions Inside Container

```bash
docker run -it --rm genomehubs-prefect-flows:latest python -c "
import tol
import click
import pydantic

print(f'tol-sdk installed')
print(f'Click: {click.__version__}')
print(f'Pydantic: {pydantic.__version__}')
"
```

### Verify Flow Runs Directly

```bash
# Test with SKIP_PREFECT=true
docker run -it --rm \
  -v $(pwd):/app \
  -e SKIP_PREFECT=true \
  genomehubs-prefect-flows:latest \
  python flows/updaters/update_tol_genome_notes.py \
  --output-path /tmp/test.tsv
```

### View Container Logs

```bash
docker-compose logs -f prefect-flows
```

### Check Exit Codes

```bash
docker run -it --rm \
  -v $(pwd):/app \
  -e SKIP_PREFECT=true \
  genomehubs-prefect-flows:latest \
  python flows/updaters/update_tol_genome_notes.py \
  --output-path /tmp/test.tsv

echo "Exit code: $?"  # Shows 0 for success, non-zero for failure
```

## Long-term Solutions

1. **Monitor tol-sdk Updates**: When newer versions of tol-sdk relax pydantic constraints
2. **Orchestration Pattern**: Use the provided orchestration layer (Option 3 above) for clean separation
3. **Dependency Pinning**: Add version constraints to `pyproject.toml` for reproducibility
4. **Multiple Environments**: Maintain separate environments for flows with conflicting dependencies (already provided via Docker)

## Orchestration Flow Template

The [flows/orchestration/tol_data_orchestration.py](flows/orchestration/tol_data_orchestration.py) file provides:

- `run_docker_flow()` task: Run any flow in Docker with proper error handling
- `orchestrate_tol_genome_notes()` flow: Complete orchestration for ToL genome notes
- `full_tol_data_pipeline()` flow: Example of chaining multiple flows
- `emit_completion_event()` task: Trigger downstream flows via Prefect events

You can extend these for other flows:

```python
# Add to tol_data_orchestration.py
@flow(name="another-orchestration")
def orchestrate_another_flow(output_path: str):
    result = run_docker_flow(
        flow_name="another-flow",
        flow_script="flows/updaters/another_flow.py",
        output_path=output_path,
    )
    emit_completion_event("another-flow", result)
    return result
```

## Environment File

The provided `env.yaml` (conda) and `requirements-docker.txt` (pip) work together:

- **env.yaml**: Base environment with system tools (ncbi-datasets, blobtk, Python 3.12)
- **requirements-docker.txt**: Python packages with pinned versions for tol-sdk compatibility

## Additional Resources

- [Prefect Docker Work Pools](https://docs.prefect.io/latest/concepts/work-pools/#docker)
- [tol-sdk GitHub](https://github.com/tree-of-life/tol-sdk)
- [Prefect Deployment Documentation](https://docs.prefect.io/latest/concepts/deployments/)
