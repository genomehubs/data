"""Parse legislation source TSV using a YAML schema.

Handles the ``FILE_*.types.yaml`` configurations under
``sources/uk-legislation`` (Council Directive 92/43/EEC, Conservation
of Habitats and Species Regulations 2017, Wildlife and Countryside
Act 1981, etc.). Delegates to the shared generic flat-TSV pipeline.
"""

import os

from flows.lib.conditional_import import flow
from flows.lib.utils import Parser, run_generic_tsv_parser
from flows.parsers.args import parse_args


@flow(log_prints=True)
def parse_legislation(
    working_yaml: str,
    work_dir: str,
    append: bool = False,
    **kwargs,
) -> None:
    """Parse a legislation TSV using YAML schema.

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
        name="LEGISLATION",
        func=parse_legislation,
        description="Parse a legislation TSV using a YAML schema.",
    )


if __name__ == "__main__":
    args = parse_args("Parse a legislation TSV using a YAML schema.")
    parse_legislation(
        working_yaml=args.yaml_path,
        work_dir=os.path.dirname(args.input_path) or ".",
        append=args.append,
    )
