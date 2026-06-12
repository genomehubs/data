"""Parse BlobToolKit assembly summary TSV.

The corresponding updater (``update_blobtoolkit``) emits a flat TSV
(one row per BTK dataset) whose column headers already match the
``header:`` values in ``btk.types.yaml``. This parser simply applies
the YAML parse functions and writes the canonical TSV.
"""

import os
from glob import glob

from flows.lib.conditional_import import flow
from flows.lib.utils import (  # noqa: E402
    Parser,
    load_config,
    parse_tsv_with_config,
    write_parsed_tsv,
)
from flows.parsers.args import parse_args  # noqa: E402


def _locate_input_tsv(work_dir: str, expected_name: str) -> str:
    """Find the input TSV in ``work_dir``.

    Args:
        work_dir (str): Working directory.
        expected_name (str): YAML-defined output filename.

    Returns:
        str: Path to the input TSV.
    """
    candidates = sorted(
        glob(os.path.join(work_dir, "*.tsv"))
        + glob(os.path.join(work_dir, "*.tsv.gz"))
    )
    candidates = [c for c in candidates if os.path.basename(c) != expected_name]
    if not candidates:
        raise FileNotFoundError(f"No BTK input TSV found in {work_dir}")
    if len(candidates) > 1:
        raise ValueError(f"Multiple TSV inputs in {work_dir}: {candidates!r}")
    return candidates[0]


@flow(log_prints=True)
def parse_blobtoolkit(
    working_yaml: str,
    work_dir: str,
    append: bool = False,
    **kwargs,
) -> None:
    """Parse BTK summary TSV using YAML schema.

    Args:
        working_yaml (str): Path to the YAML configuration file.
        work_dir (str): Working directory containing the input TSV.
        append (bool): If True, load previous parsed data.
        **kwargs: Ignored extra arguments from the wrapper.
    """
    config = load_config(config_file=working_yaml, load_previous=append)

    expected_name = config.meta["file_name"]
    input_path = _locate_input_tsv(work_dir, expected_name)
    print(f"Parsing BlobToolKit summary: {input_path}")

    parsed = parse_tsv_with_config(input_path, config, key_field="accession")
    print(f"Parsed {len(parsed)} BTK dataset records")

    output_name = config.meta["file_name"]
    config.meta["file_name"] = os.path.join(
        work_dir, os.path.basename(output_name)
    )
    try:
        write_parsed_tsv(parsed, config)
    finally:
        config.meta["file_name"] = output_name


def plugin():
    """Register the parser plugin."""
    return Parser(
        name="BLOBTOOLKIT",
        func=parse_blobtoolkit,
        description="Parse BlobToolKit assembly summary TSV using YAML schema.",
    )


if __name__ == "__main__":
    args = parse_args("Parse BlobToolKit assembly summary TSV.")
    parse_blobtoolkit(
        working_yaml=args.yaml_path,
        work_dir=os.path.dirname(args.input_path) or ".",
        append=args.append,
    )
