"""Parse status list TSVs using a YAML schema.

Generic parser that handles all sequencing status list inputs:
VGP, JGI 1KFG, Google Sheets project lists, NHM, CNGB,
ToL Portal, ToL Genome Notes, and similar.

The input is a tab-separated file produced by the corresponding
updater (one row per record). The YAML schema describes how each
input column maps to a GoaT attribute (and may translate values
via ``translate:`` blocks). This parser:

1. Locates the input TSV in ``work_dir`` matching ``meta.file_name``
   (or, failing that, the single TSV in the directory).
2. Reads each row as a flat dict keyed by column header.
3. Applies YAML parse functions via ``gh_utils.parse_report_values``.
4. Writes the canonical TSV using YAML-defined headers.
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

    Looks first for ``expected_name`` (matching ``meta.file_name`` from
    the YAML); falls back to a single ``*.tsv`` or ``*.tsv.gz`` in
    ``work_dir`` that is not the expected output.

    Args:
        work_dir (str): Working directory.
        expected_name (str): The filename declared in YAML ``file.name``.

    Returns:
        str: Absolute path to the input TSV.
    """
    expected_path = os.path.join(work_dir, expected_name)
    if os.path.exists(expected_path):
        return expected_path

    candidates = sorted(
        glob(os.path.join(work_dir, "*.tsv"))
        + glob(os.path.join(work_dir, "*.tsv.gz"))
    )
    if not candidates:
        raise FileNotFoundError(
            f"No TSV input found in {work_dir} (expected {expected_name})"
        )
    if len(candidates) > 1:
        raise ValueError(
            f"Multiple TSV inputs in {work_dir}: {candidates!r}; "
            "place a single source TSV or name it to match YAML file.name."
        )
    return candidates[0]


@flow(log_prints=True)
def parse_sequencing_status(
    working_yaml: str,
    work_dir: str,
    append: bool = False,
    **kwargs,
) -> None:
    """Parse a sequencing status TSV using a YAML schema.

    Args:
        working_yaml (str): Path to the YAML configuration file.
        work_dir (str): Working directory containing the input TSV.
        append (bool): If True, load previous parsed data for incremental
            updates.
        **kwargs: Ignored extra arguments from the wrapper.
    """
    config = load_config(config_file=working_yaml, load_previous=append)

    expected_name = config.meta["file_name"]
    input_path = _locate_input_tsv(work_dir, expected_name)
    print(f"Parsing sequencing status: {input_path}")

    parsed = parse_tsv_with_config(input_path, config)
    print(f"Parsed {len(parsed)} rows")

    output_name = config.meta["file_name"]
    config.meta["file_name"] = os.path.join(work_dir, os.path.basename(output_name))
    try:
        write_parsed_tsv(parsed, config)
    finally:
        config.meta["file_name"] = output_name


def plugin():
    """Register the parser plugin."""
    return Parser(
        name="SEQUENCING_STATUS",
        func=parse_sequencing_status,
        description="Parse a sequencing status TSV using a YAML schema.",
    )


if __name__ == "__main__":
    args = parse_args("Parse a sequencing status TSV using a YAML schema.")
    parse_sequencing_status(
        working_yaml=args.yaml_path,
        work_dir=os.path.dirname(args.input_path) or ".",
        append=args.append,
    )
