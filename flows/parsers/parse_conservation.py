"""Parse conservation-status source TSV using a YAML schema.

Handles the ``FILE_CITES_full_index.types.yaml`` (and any future
``FILE_*.types.yaml``) configurations under ``sources/conservation``.
Delegates to the shared generic flat-TSV pipeline.
"""

import os

from flows.lib.conditional_import import flow
from flows.lib.utils import Parser, run_generic_tsv_parser
from flows.parsers.args import parse_args


@flow(log_prints=True)
def parse_conservation(
    working_yaml: str,
    work_dir: str,
    append: bool = False,
    **kwargs,
) -> None:
    """Parse a conservation-status TSV using YAML schema.

    Args:
        working_yaml (str): Path to the YAML configuration file.
        work_dir (str): Working directory containing the input TSV.
        append (bool): If True, load previous parsed data.
        **kwargs: Ignored extra arguments from the wrapper.
    """
    run_generic_tsv_parser(
        working_yaml=working_yaml, work_dir=work_dir, append=append
    )


def plugin():
    """Register the parser plugin."""
    return Parser(
        name="CONSERVATION",
        func=parse_conservation,
        description="Parse a conservation-status TSV using a YAML schema.",
    )


if __name__ == "__main__":
    args = parse_args("Parse a conservation-status TSV using a YAML schema.")
    parse_conservation(
        working_yaml=args.yaml_path,
        work_dir=os.path.dirname(args.input_path) or ".",
        append=args.append,
    )
