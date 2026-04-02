# Contributing to GenomeHubs Data

Welcome! This guide helps you contribute code to the GenomeHubs data pipeline. We've established clear conventions to keep the codebase consistent, maintainable, and easy to test locally.

## Quick Navigation

- **New to the project?** → Read [Project Structure](#project-structure)
- **Writing your first parser?** → Jump to [Creating a New Parser](#creating-a-new-parser)
- **Adding an updater flow?** → See [Creating an Updater](#creating-an-updater)
- **Detailed conventions?** → Check [.copilot-instructions.md](.copilot-instructions.md)
- **Using AI assistance?** → See [.agent.md](.agent.md)

---

## Project Structure

The `flows/` directory organizes code into four main categories:

```
flows/
├── lib/              # Shared utilities (Config, utils, shared args)
├── parsers/          # Transform raw data → GenomeHubs TSV (plugin-based)
├── updaters/         # Fetch remote data (NCBI, ENA, etc.)
├── validators/       # Validate data quality and schema
└── feature_parsers/  # Handle feature-specific data
```

### What Each Category Does

| Category       | Purpose                                            | Example                                   | Emits Events? |
| -------------- | -------------------------------------------------- | ----------------------------------------- | ------------- |
| **Parsers**    | Transform raw input data into GenomeHubs-ready TSV | NCBI JSONL → assembly TSV                 | No            |
| **Updaters**   | Fetch data from remote sources, write to disk      | Query NCBI → local JSON                   | **Yes**       |
| **Validators** | Check data quality against schema                  | Validate TSV vs YAML                      | Yes           |
| **lib/**       | Shared utilities used by all flows                 | Config class, safe_get(), parse_s3_file() | —             |

---

## Local Setup

### Prerequisites

```bash
# Install dependencies
conda env create -f env.yaml
conda activate genomehubs_data
```

**Note:** S3 access is only needed for deployment/production runs. Local testing can skip it.

### Running Flows Locally

All flows can be tested without a Prefect server using `SKIP_PREFECT=true`. For local testing, omit S3 paths:

```bash
# Test any flow's help
SKIP_PREFECT=true python3 -m flows.lib.validate_file_pair -h

# Run a parser locally
SKIP_PREFECT=true python3 -m flows.parsers.parse_ncbi_assemblies \
  -i input.jsonl \
  -y config.yaml

# Run an updater locally (outputs to local file)
SKIP_PREFECT=true python3 -m flows.updaters.update_ncbi_datasets \
  -r 2759 \
  -o /tmp/output.jsonl

# Run a validator locally
SKIP_PREFECT=true python3 -m flows.validators.validate_file_pair \
  -y config.yaml \
  -w /tmp
```

**S3 paths are optional for local testing and mostly used during deployment.**

---

## Core Conventions

### 1. Always Use Absolute Imports

All internal imports must use the `flows.` package prefix to work both with `python -m` and Prefect deployments:

```python
# ✓ Correct
from flows.lib import utils
from flows.lib.conditional_import import flow, task
from flows.lib.shared_args import parse_args, required

# ✗ Wrong (will fail with python -m)
import utils
from conditional_import import flow
```

### 2. Write Docstrings in Google Format

Every public function needs a docstring:

```python
def validate_accession(accession: str) -> bool:
    """
    Check if accession is valid NCBI format.

    Args:
        accession (str): NCBI accession, e.g., 'GCA_000001405.30'.

    Returns:
        bool: True if valid.

    Raises:
        ValueError: If accession format is invalid.
    """
    if not accession or '.' not in accession:
        raise ValueError(f"Invalid format: {accession}")
    return True
```

### 3. Keep Functions Focused and Simple

- One responsibility per function
- Max 2–3 levels of nesting (if/for/while)
- Minimize side effects in utility functions

```python
# Good: Single responsibility
@task()
def extract_accession_version(accession: str) -> tuple:
    """Extract version from NCBI accession."""
    base, version = accession.rsplit('.', 1)
    return (base, int(version))

# Avoid: Too many concerns mixed
@task()
def process_all_records(records, config):
    # Parsing, validation, transformation, writing... all in one place
```

### 4. Use Dict-Based CLI Arguments

All CLI arguments are defined centrally. Don't create your own parsers!

**In your flow file:**

```python
from flows.parsers.args import parse_args

if __name__ == "__main__":
    args = parse_args("Your flow description here.")
    my_flow(**vars(args))
```

**In a new subdirectory, create `args.py`:**

```python
# flows/my_new_directory/args.py
from flows.lib.shared_args import INPUT_PATH, YAML_PATH, required, parse_args as _parse_args

def parse_args(description="My parser"):
    return _parse_args([required(INPUT_PATH), required(YAML_PATH)], description)
```

---

## Creating a New Parser

Parsers transform raw input into GenomeHubs TSV format using YAML configuration files.

### Step 1: Create the Flow File

Make a new file in `flows/parsers/parse_<name>.py`:

```python
"""Parse <description>."""

from flows.lib.conditional_import import flow, task
from flows.lib.utils import Config, Parser, load_config
from flows.parsers.args import parse_args


@task()
def process_record(record: dict, config: Config) -> dict:
    """
    Transform a single record.

    Args:
        record (dict): Raw input record.
        config (Config): YAML configuration.

    Returns:
        dict: Transformed record.
    """
    # Extract fields from record based on config
    return {...}


@flow()
def parse_my_data(
    working_yaml: str,
    work_dir: str,
    append: bool,
    data_freeze_path: str = None,
    **kwargs
) -> None:
    """
    Parse my custom data format into GenomeHubs TSV.

    Args:
        working_yaml (str): Path to YAML configuration.
        work_dir (str): Working directory for output.
        append (bool): Append to existing TSV.
        data_freeze_path (str): Optional frozen dataset list.
        **kwargs: Additional pipeline arguments.
    """
    config = load_config(working_yaml)

    # Parse input and write output
    # Output filename derives from config: config.meta['file_name']
    parsed = {}
    for record in read_input():
        parsed[record['id']] = process_record(record, config)

    # Write TSV
    output_path = os.path.join(work_dir, config.meta['file_name'])
    write_tsv(output_path, parsed, config)


def plugin():
    """Register this parser for plugin discovery."""
    return Parser(
        name="MY_PARSER",
        func=parse_my_data,
        description="Parse my data format into GenomeHubs TSV.",
    )


if __name__ == "__main__":
    args = parse_args("Parse my data format.")
    parse_my_data(**vars(args))
```

### Step 2: Test Locally

```bash
SKIP_PREFECT=true python3 -m flows.parsers.parse_my_data \
  -i input.jsonl \
  -y config.yaml \
  -a  # append flag
```

### Step 3: Verify Plugin Registration

Your parser is automatically discovered via the `plugin()` function. Test it:

```bash
SKIP_PREFECT=true python3 -m flows.lib.wrapper_fetch_parse_validate \
  -p MY_PARSER \
  -y config.yaml \
  -s s3://bucket/path \
  -i input.jsonl
```

---

## Creating an Updater

Updaters fetch data from remote sources and write to local disk. They emit events to trigger downstream flows.

### Step 1: Create the Flow File

Make a new file in `flows/updaters/update_<source>.py`:

```python
"""Fetch data from <source>."""

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.shared_args import OUTPUT_PATH, ROOT_TAXID, default, parse_args, required
from flows.lib.utils import run_quoted


@task()
def fetch_and_write(root_taxid: str, output_path: str) -> int:
    """
    Fetch data and write to file.

    Args:
        root_taxid (str): Root taxonomy ID for filtering.
        output_path (str): Path to write output.

    Returns:
        int: Number of records written.
    """
    # Use external tools or APIs
    result = run_quoted(
        ["my-tool", "fetch", "--taxid", root_taxid],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Fetch failed: {result.stderr}")

    # Write results
    with open(output_path, "w") as f:
        f.write(result.stdout)

    return len(result.stdout.splitlines())


@flow()
def update_my_source(
    root_taxid: str,
    output_path: str,
    s3_path: str = None  # Optional, used in production only
) -> None:
    """
    Fetch data from source and emit completion event.

    Args:
        root_taxid (str): Root taxonomy ID.
        output_path (str): Local path to write output.
        s3_path (str): Optional S3 path for production deployments.
    """
    line_count = fetch_and_write(root_taxid, output_path)

    # Always emit event for downstream flows
    emit_event(
        event="update.my_source.completed",
        resource={
            "prefect.resource.id": f"update.my_source.{root_taxid}",
            "prefect.resource.type": "update.my_source",
        },
        payload={"line_count": line_count, "status": "success"},
    )


if __name__ == "__main__":
    from flows.lib.shared_args import parse_args

    args = parse_args(
        [required(ROOT_TAXID), required(OUTPUT_PATH), default(S3_PATH, None)],
        "Fetch data from my source."
    )
    update_my_source(**vars(args))
```

### Step 2: Test Locally

```bash
SKIP_PREFECT=true python3 -m flows.updaters.update_my_source \
  -r 2759 \
  -o /tmp/output.jsonl
```

### Handling Incompatible Dependencies: Orchestrators

**Only use orchestrators if your updater introduces dependencies that conflict with Prefect.** This is a rare edge case (e.g., tol-sdk requires Pydantic < 2.0, but Prefect needs 2.0+).

❌ **DO NOT** try to force incompatible dependencies into standard Prefect environment.
❌ **DO NOT** create orchestrators for updaters that work fine in Prefect.
⚠️ **REMEMBER:** Orchestrators add complexity. Only use when absolutely necessary.

**If your updater has incompatible dependencies:**

1. Create the updater normally in `flows/updaters/` (it will fail in Prefect, but works with `SKIP_PREFECT=true`)
2. Create an orchestrator wrapper in `flows/orchestrators/`:

```python
# flows/orchestrators/my_updater_orchestration.py
from flows.orchestrators.tasks import run_docker_flow, emit_completion_event
from prefect import flow, get_run_logger

@flow(name="my-updater-orchestration")
def orchestrate_my_updater(
    output_path: str = "/tmp/my_output.tsv",
    s3_path: str = None,
    min_records: int = 1,
) -> dict:
    """Run my_updater in Docker to avoid dependency conflicts."""
    logger = get_run_logger()

    result = run_docker_flow(
        flow_name="my-updater",
        flow_script="flows/updaters/update_my_source.py",
        output_path=output_path,
        s3_path=s3_path,
        min_records=min_records,
    )

    emit_completion_event(
        flow_name="my-updater",
        result=result,
        resource_type="my.updater",
    )

    logger.info(f"🎉 Orchestration complete: {result}")
    return result
```

3. Register in `flows/prefect.yaml` instead of the raw updater:

```yaml
- name: orchestrate-my-updater
  entrypoint: flows/orchestrators/my_updater_orchestration.py:orchestrate_my_updater
  parameters:
    output_path: "/home/ubuntu/tmp/test/my_output.tsv"
    s3_path: s3://goat/resources/my_output.tsv
    min_records: 100
```

**Key constraint:** Orchestrators are **ONLY for updaters**. Parsers and validators must never introduce dependencies incompatible with Prefect/GenomeHubs.

---

## Creating a Validator

Validators check data quality and schema compliance. They follow the pattern in `flows/validators/`.

### Step 1: Create the Flow File

Make a new file in `flows/validators/validate_<name>.py`:

```python
"""Validate <data type> quality and schema."""

from flows.lib.conditional_import import emit_event, flow, task
from flows.lib.utils import load_config
from flows.validators.args import parse_args


@task()
def check_schema(yaml_path: str, work_dir: str) -> bool:
    """Check TSV against YAML schema."""
    config = load_config(yaml_path)
    # Validation logic
    return True


@flow()
def validate_my_data(
    yaml_path: str,
    work_dir: str,
    taxdump_path: str = None,
    s3_path: str = None,  # Optional, production/deployment only
    min_valid: int = 1,
    min_assigned: int = 1
) -> bool:
    """
    Validate data against schema.

    Args:
        yaml_path (str): YAML schema configuration.
        work_dir (str): Directory containing data to validate.
        taxdump_path (str): Optional taxonomy for semantic validation.
        s3_path (str): Optional S3 path for validated output (production).
        min_valid (int): Minimum valid rows required.
        min_assigned (int): Minimum assigned taxa required.

    Returns:
        bool: True if validation passed.
    """
    passed = check_schema(yaml_path, work_dir)

    # Emit event
    emit_event(
        event="validate.my_data.completed",
        resource={"prefect.resource.id": f"validate.{yaml_path}"},
        payload={"passed": passed},
    )

    return passed


if __name__ == "__main__":
    args = parse_args("Validate my data.")
    validate_my_data(**vars(args))
```

### Step 2: Test Locally

```bash
SKIP_PREFECT=true python3 -m flows.validators.validate_my_data \
  -y config.yaml \
  -w /tmp
```

---

## Adding Shared Utilities

When you find yourself repeating code across multiple flows, extract it to `flows/lib/utils.py`:

```python
# flows/lib/utils.py

def extract_tax_lineage(organism_name: str) -> list:
    """
    Extract taxonomic lineage from organism name.

    Args:
        organism_name (str): Organism name, e.g., 'Homo sapiens'.

    Returns:
        list: Lineage components.

    Raises:
        ValueError: If organism format is invalid.
    """
    # Implementation
    return []


# In your parser:
from flows.lib.utils import extract_tax_lineage

lineage = extract_tax_lineage(record['organism'])
```

---

## Code Review Checklist

Before submitting a PR, verify:

- [ ] **Imports**: All internal imports use `flows.` prefix (absolute paths)
- [ ] **Docstrings**: All public functions have Google-format docstrings with types
- [ ] **Function complexity**: No function has >3 levels of nesting
- [ ] **CLI args**: Uses dict-based args from `shared_args.py`
- [ ] **Testing**: Tested locally with `SKIP_PREFECT=true python3 -m flows...`
- [ ] **Type hints**: All function signatures include type annotations
- [ ] **Error handling**: Descriptive error messages; validates inputs early
- [ ] **Side effects**: Utility functions are pure or clearly documented

### Parser-Specific

- [ ] Has `plugin()` function returning `Parser(...)`
- [ ] Function signature matches standard: `parse_<name>(working_yaml, work_dir, append, **kwargs)`
- [ ] Output path derived from config: `config.meta['file_name']`

### Updater-Specific

- [ ] Emits completion event with `emit_event()`
- [ ] Event name follows pattern: `update.<source>.completed`
- [ ] Event resource includes unique ID and type

### Validator-Specific

- [ ] Located in `flows/validators/`
- [ ] Uses standard args from `flows/validators/args.py`
- [ ] Returns `bool` indicating validation status

---

## Testing Strategies

### Quick Smoke Test

```bash
# Verify no import errors
SKIP_PREFECT=true python3 -m flows.<category>.<module> -h
```

### Functional Test with Sample Data

```bash
# Test with small dataset
SKIP_PREFECT=true python3 -m flows.parsers.parse_my_data \
  -i test_input.jsonl \
  -y test_config.yaml \
  -w /tmp/test_output
```

### Integration Test

```bash
# Test full pipeline (local paths only; S3 is optional)
SKIP_PREFECT=true python3 -m flows.lib.wrapper_fetch_parse_validate \
  -p ncbi_assemblies \
  -y config.yaml \
  -w /tmp/work
```

---

## Common Patterns

### Pattern: Safe Path Validation

```python
from flows.lib import utils

if not utils.is_safe_path(user_input):
    raise ValueError(f"Unsafe path: {user_input}")
```

### Pattern: Generator for Memory Efficiency

```python
@task()
def process_large_file(file_path: str):
    """Yield records to avoid loading entire file in memory."""
    with open(file_path) as f:
        for line in f:
            record = json.loads(line)
            yield process_record(record)
```

### Pattern: Logging with Prefect

Tasks automatically capture `print()` output:

```python
@task(log_prints=True)
def my_task():
    print("This shows in Prefect logs")
    for i in range(10):
        print(f"  Processing {i}")
```

### Pattern: S3 Operations (Deployment Only)

When deploying to production:

```python
from flows.lib.utils import parse_s3_file, safe_get

# Download via HTTP
response = safe_get("https://example.com/data.tsv")
data = response.text

# Parse S3 file (production only)
data = parse_s3_file("s3://bucket/path/file.tsv")
```

---

## Getting Help

- **Questions about conventions?** Check [.copilot-instructions.md](.copilot-instructions.md)
- **Working with AI?** See [.agent.md](.agent.md)
- **Need examples?** Look at existing parsers in `flows/parsers/`
- **Want to debug?** Run locally with `SKIP_PREFECT=true`

---

## Deployment

Once your flow is tested locally and passes code review, it's ready for production deployment (requires S3 and Prefect server access—typically handled by maintainers):

1. Ensure code passes all local tests with `SKIP_PREFECT=true`
2. Submit PR for review
3. Maintainer creates deployment in `flows/prefect.yaml`
4. Maintainer deploys to production:
   ```bash
   prefect deploy --prefect-file flows/prefect.yaml
   ```
5. Flows trigger via Prefect UI or API

**Contributors:** You don't need S3 or Prefect server access. Focus on local testing.

---

## Summary

The key principles for contributors:

1. **Absolute imports** — Always use `flows.` prefix
2. **Docstrings everywhere** — Google format, all public functions
3. **One job per function** — Keep it simple; max 3 nesting levels
4. **Dict-based CLI args** — No custom argparse; use `shared_args.py`
5. **Test locally first** — Use `SKIP_PREFECT=true` before submitting PR
6. **Categorize correctly** — Parsers (transform), Updaters (fetch+emit), Validators (check)

**S3 and Prefect deployment are handled by maintainers—focus on code quality and local testing.**

---

**Questions or suggestions?** Feel free to open an issue or PR!
