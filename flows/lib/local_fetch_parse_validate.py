"""Local fetch-parse-validate wrapper for testing without S3.

This mirrors the production wrapper_fetch_parse_validate.py but:
- Skips S3 fetch (uses a local input TSV directly)
- Copies YAML + input TSV into work_dir with expected names
- Runs the selected parser (or SKIP_PARSING)
- Runs blobtk validate locally (no S3 upload)

Usage:
    SKIP_PREFECT=true python -m flows.lib.local_fetch_parse_validate \
        -p SKIP_PARSING \
        --yaml-path ../goat-data/sources/status-lists/FILE_VGP_Ordinal_Phase1.types.yaml \
        --input-tsv tsv_examples/VGP_Ordinal_Phase1_plus.tsv \
        --work-dir /tmp/test-vgp

    SKIP_PREFECT=true python -m flows.lib.local_fetch_parse_validate \
        -p REFSEQ_ORGANELLES \
        --yaml-path ../goat-data/sources/assembly-data/refseq_organelles.types.yaml \
        --input-tsv tsv_examples/refseq_organelles.tsv \
        --work-dir /tmp/test-refseq
"""

import argparse
import os
import shutil
import subprocess
import sys

from flows.lib.conditional_import import flow
from flows.lib.utils import enum_action, load_config
from flows.parsers.register import register_plugins
from flows.validators.validate_file_pair import validate_file_pair

PARSERS = register_plugins()


def _check_blobtk():
    """Verify blobtk is available on PATH."""
    try:
        subprocess.run(
            ["blobtk", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        print(
            "[local] WARNING: 'blobtk' not found on PATH. "
            "Validation step will be skipped.\n"
            "       Install via: cd blobtk/rust && cargo build --release && "
            "export PATH=$PWD/target/release:$PATH",
            file=sys.stderr,
        )
        return False
    return True


def _copy_yaml_to_workdir(yaml_path: str, work_dir: str) -> str:
    """Copy the YAML and its dependencies into work_dir.

    Returns:
        str: Path to the copied YAML in work_dir.
    """
    os.makedirs(work_dir, exist_ok=True)
    config = load_config(yaml_path)
    dest = os.path.join(work_dir, os.path.basename(yaml_path))
    shutil.copy(yaml_path, dest)

    # Copy dependency YAML files (e.g. "needs:" references)
    if "needs" in config.config.get("file", {}):
        source_dir = os.path.dirname(yaml_path)
        needs = config.config["file"]["needs"]
        if isinstance(needs, str):
            needs = [needs]
        for dep in needs:
            dep_path = os.path.join(source_dir, dep)
            if os.path.exists(dep_path):
                shutil.copy(dep_path, work_dir)
    return dest


def _place_input_tsv(input_tsv: str, yaml_path: str, work_dir: str) -> str:
    """Copy or symlink the input TSV into work_dir with the name expected by the YAML.

    Handles gzip: if YAML expects .gz but input is plain, compress on copy.
    If YAML expects plain but input is .gz, decompress on copy.

    Returns:
        str: Path to the TSV in work_dir.
    """
    import gzip as gzip_mod

    config = load_config(yaml_path)
    expected_name = os.path.basename(config.config["file"]["name"])
    dest = os.path.join(work_dir, expected_name)

    # If input already matches expected location, skip
    if os.path.abspath(input_tsv) == os.path.abspath(dest):
        return dest

    expects_gz = expected_name.endswith(".gz")
    input_is_gz = input_tsv.endswith(".gz")

    if expects_gz and not input_is_gz:
        # Compress plain input into .gz destination
        with open(input_tsv, "rb") as f_in, gzip_mod.open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    elif not expects_gz and input_is_gz:
        # Decompress .gz input into plain destination
        with gzip_mod.open(input_tsv, "rb") as f_in, open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    else:
        # Same format — straight copy
        shutil.copy(input_tsv, dest)
    return dest


@flow(log_prints=True)
def local_fetch_parse_validate(
    parser: str,
    yaml_path: str,
    input_tsv: str,
    work_dir: str,
    taxdump_path: str = None,
    append: bool = False,
    min_valid: int = 0,
    min_assigned: int = 0,
) -> bool:
    """Run the parse-validate pipeline locally without S3.

    Args:
        parser: Parser enum name (e.g. "SKIP_PARSING", "REFSEQ_ORGANELLES").
        yaml_path: Path to the source YAML configuration file.
        input_tsv: Path to the input TSV file from the updater.
        work_dir: Working directory for intermediate files.
        taxdump_path: Optional path to an NCBI taxdump for taxonomy validation.
        append: Whether to run in append mode.
        min_valid: Minimum expected valid row count.
        min_assigned: Minimum expected assigned taxa count.

    Returns:
        bool: True if validation passed.
    """
    yaml_path = os.path.abspath(yaml_path)
    input_tsv = os.path.abspath(input_tsv)
    work_dir = os.path.abspath(work_dir)

    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"YAML file not found: {yaml_path}")
    if not os.path.exists(input_tsv):
        raise FileNotFoundError(f"Input TSV not found: {input_tsv}")

    print(f"[local] Parser: {parser}")
    print(f"[local] YAML:   {yaml_path}")
    print(f"[local] Input:  {input_tsv}")
    print(f"[local] Work:   {work_dir}")

    # Step 1: Copy YAML to work_dir
    working_yaml = _copy_yaml_to_workdir(yaml_path, work_dir)
    print(f"[local] Copied YAML → {working_yaml}")

    # Step 2: Place input TSV with expected filename
    tsv_dest = _place_input_tsv(input_tsv, yaml_path, work_dir)
    print(f"[local] Input TSV → {tsv_dest}")

    # Step 3: Run parser
    parser_key = parser.name if hasattr(parser, "name") else str(parser)
    file_parser = PARSERS.parsers[parser_key]
    print(f"[local] Running parser: {file_parser.name}")
    file_parser.func(
        working_yaml=working_yaml,
        work_dir=work_dir,
        append=append,
        data_freeze_path=None,
    )
    print("[local] Parser completed")

    # Step 4: Validate (no S3 upload — s3_path=None)
    if _check_blobtk():
        print("[local] Running validation...")
        status = validate_file_pair(
            yaml_path=yaml_path,
            work_dir=work_dir,
            taxdump_path=taxdump_path,
            s3_path=None,
            min_valid=min_valid,
            min_assigned=min_assigned,
        )
        if status:
            print("[local] ✓ Validation PASSED")
        else:
            print("[local] ✗ Validation FAILED")
    else:
        print("[local] ⚠ Validation SKIPPED (blobtk not available)")
        status = None
    return status


def main():
    """CLI entry point."""
    arg_parser = argparse.ArgumentParser(
        description="Local fetch-parse-validate (no S3).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    arg_parser.add_argument(
        "-p", "--parser",
        required=True,
        type=str,
        action=enum_action(PARSERS.ParserEnum),
        help=f"Parser to use. Choices: {[e.name for e in PARSERS.ParserEnum]}",
    )
    arg_parser.add_argument(
        "--yaml-path",
        required=True,
        help="Path to the source YAML configuration file.",
    )
    arg_parser.add_argument(
        "--input-tsv",
        required=True,
        help="Path to the input TSV file (from the updater).",
    )
    arg_parser.add_argument(
        "--work-dir",
        default="/tmp/local-fpv",
        help="Working directory for intermediate files.",
    )
    arg_parser.add_argument(
        "--taxdump-path",
        default=None,
        help="Path to an NCBI taxdump directory.",
    )
    arg_parser.add_argument(
        "--append",
        action="store_true",
        help="Run in append mode.",
    )
    arg_parser.add_argument(
        "--min-valid",
        type=int,
        default=0,
        help="Minimum expected valid row count.",
    )
    arg_parser.add_argument(
        "--min-assigned",
        type=int,
        default=0,
        help="Minimum expected assigned taxa count.",
    )

    args = arg_parser.parse_args()
    success = local_fetch_parse_validate(
        parser=args.parser,
        yaml_path=args.yaml_path,
        input_tsv=args.input_tsv,
        work_dir=args.work_dir,
        taxdump_path=args.taxdump_path,
        append=args.append,
        min_valid=args.min_valid,
        min_assigned=args.min_assigned,
    )
    # Exit 0 if validation passed or was skipped (None), 1 if failed
    sys.exit(0 if success is not False else 1)


if __name__ == "__main__":
    main()
