"""Parse RefSeq organelle data into one-row-per-assembly TSV.

The corresponding updater (``update_refseq_organelles``) emits one row per
organelle sequence (mitochondrion or plastid). The GoaT YAML schema
(``refseq_organelles.types.yaml``) expects one row per assembly with
combined ``mitochondrion*`` / ``plastid*`` columns. This parser pivots
the per-organelle rows by the assembly accession (genbank), then runs
the records through the YAML parse functions.
"""

import os
from csv import DictReader
from glob import glob

from genomehubs import utils as gh_utils

from flows.lib.conditional_import import flow
from flows.lib.utils import (  # noqa: E402
    Parser,
    load_config,
    open_tsv,
    write_parsed_tsv,
)
from flows.parsers.args import parse_args  # noqa: E402

ORGANELLE_FIELDS = ("id", "assemblySpan", "gcPercent", "nPercent")


def _locate_input_tsv(work_dir: str, expected_name: str) -> str:
    """Find the per-organelle input TSV in ``work_dir``."""
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
            f"Multiple TSV inputs in {work_dir}: {candidates!r}"
        )
    return candidates[0]


def _pivot_by_assembly(input_path: str) -> dict:
    """Group per-organelle rows by GenBank accession.

    Args:
        input_path (str): Path to the per-organelle TSV.

    Returns:
        dict: Mapping of assembly accession → nested record with
            ``mitochondrion``/``plastid`` sub-dicts.
    """
    by_assembly: dict = {}
    with open_tsv(input_path) as fh:
        reader = DictReader(fh, delimiter="\t")
        for row in reader:
            assembly = row.get("genbankAccession") or row.get("id")
            if not assembly:
                continue
            record = by_assembly.setdefault(
                assembly,
                {
                    "id": row.get("id", assembly),
                    "genbankAccession": assembly,
                    "bioproject": row.get("bioproject", ""),
                    "biosample": row.get("biosample", ""),
                    "releaseDate": row.get("releaseDate", ""),
                    "annotations": {"organism": row.get("organismName", "")},
                    "taxonId": row.get("taxonId", ""),
                    "sourceAuthor": row.get("sourceAuthor", ""),
                    "sourceYear": row.get("sourceYear", ""),
                    "sourceTitle": row.get("sourceTitle", ""),
                    "pubmedId": row.get("pubmedId", ""),
                    "sampleLocation": row.get("sampleLocation", ""),
                },
            )
            organelle = (row.get("organelle") or "").lower()
            if organelle in ("mitochondrion", "plastid"):
                record[organelle] = {
                    field: row.get(field, "") for field in ORGANELLE_FIELDS
                }
    return by_assembly


@flow(log_prints=True)
def parse_refseq_organelles(
    working_yaml: str,
    work_dir: str,
    append: bool = False,
    **kwargs,
) -> None:
    """Pivot per-organelle TSV to per-assembly and apply YAML schema.

    Args:
        working_yaml (str): Path to the YAML configuration file.
        work_dir (str): Working directory containing the input TSV.
        append (bool): If True, load previous parsed data.
        **kwargs: Ignored extra arguments from the wrapper.
    """
    config = load_config(config_file=working_yaml, load_previous=append)

    expected_name = config.meta["file_name"]
    input_path = _locate_input_tsv(work_dir, expected_name)
    print(f"Parsing RefSeq organelles: {input_path}")

    grouped = _pivot_by_assembly(input_path)
    print(f"Pivoted to {len(grouped)} assemblies")

    parsed = {
        key: gh_utils.parse_report_values(config.parse_fns, record)
        for key, record in grouped.items()
    }

    output_name = config.meta["file_name"]
    config.meta["file_name"] = os.path.join(work_dir, os.path.basename(output_name))
    try:
        write_parsed_tsv(parsed, config)
    finally:
        config.meta["file_name"] = output_name


def plugin():
    """Register the parser plugin."""
    return Parser(
        name="REFSEQ_ORGANELLES",
        func=parse_refseq_organelles,
        description="Pivot per-organelle TSV to per-assembly and apply YAML schema.",
    )


if __name__ == "__main__":
    args = parse_args("Parse RefSeq organelle data into one-row-per-assembly TSV.")
    parse_refseq_organelles(
        working_yaml=args.yaml_path,
        work_dir=os.path.dirname(args.input_path) or ".",
        append=args.append,
    )
