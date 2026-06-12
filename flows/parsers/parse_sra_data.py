"""Parse SRA accession TSV using a YAML schema.

The corresponding updater (``update_sra_data``) emits a flat TSV with
columns matching the headers in ``sra.types.yaml`` (``run_accession``,
``sra_accession``, ``platform``, ``library_source``, ``reads``,
``total_runs``, ``total_reads``, ``taxon_id``). This parser delegates
to the shared generic flat-TSV pipeline.
"""

import os

from flows.lib.conditional_import import flow
from flows.lib.utils import Parser, run_generic_tsv_parser
from flows.parsers.args import parse_args


@flow(log_prints=True)
def parse_sra_data(
    working_yaml: str,
    work_dir: str,
    append: bool = False,
    **kwargs,
) -> None:
    """Parse SRA accession TSV using YAML schema.

    Args:
        working_yaml (str): Path to the YAML configuration file.
        work_dir (str): Working directory containing the input TSV.
        append (bool): If True, load previous parsed data.
        **kwargs: Ignored extra arguments from the wrapper.
    """
    run_generic_tsv_parser(
        working_yaml=working_yaml,
        work_dir=work_dir,
        append=append,
        key_field="run_accession",
    )


def plugin():
    """Register the parser plugin."""
    return Parser(
        name="SRA_DATA",
        func=parse_sra_data,
        description="Parse SRA accession TSV using a YAML schema.",
    )


if __name__ == "__main__":
    args = parse_args("Parse SRA accession TSV using a YAML schema.")
    parse_sra_data(
        working_yaml=args.yaml_path,
        work_dir=os.path.dirname(args.input_path) or ".",
        append=args.append,
    )
