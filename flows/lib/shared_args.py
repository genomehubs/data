#!/usr/bin/env python3
"""
Arguments shared between scripts.

Each dictionary in the list represents an argument to be added to the parser.
The dictionary contains two keys:
- flags: A list of strings representing the argument flags.
- keys: A dictionary of keyword arguments to be passed to the parser.

Example:
    S3_PATH = {"flags": ["-s", "--s3_path"], "keys": {"help": "Path to the TSV
    directory on S3.", "required": True, "type": str}}

    parser.add_argument(*S3_PATH["flags"], **S3_PATH["keys"])
"""

import argparse
import re
from typing import Any, Dict, Union

API_URL = {
    "flags": ["--api_url"],
    "keys": {
        "help": "URL of the API.",
        "type": str,
    },
}

APPEND = {
    "flags": ["-a", "--append"],
    "keys": {
        "help": "Flag to append values to an existing file(s).",
        "action": "store_true",
    },
}

ASSEMBLY_ID = {
    "flags": ["--assembly_id"],
    "keys": {
        "help": "Assembly ID.",
        "type": str,
    },
}

DATE = {
    "flags": ["--date"],
    "keys": {
        "help": "Date of the index. Format: YYYY-MM-DD.",
        "type": str,
    },
}

DRY_RUN = {
    "flags": ["-d", "--dry_run"],
    "keys": {
        "help": "Flag to perform a dry run without updating S3/git files.",
        "action": "store_true",
    },
}

HTTP_PATH = {
    "flags": ["-u", "--http_path"],
    "keys": {
        "help": "Path to the HTTP directory.",
        "type": str,
    },
}

HUB_NAME = {
    "flags": ["--hub_name"],
    "keys": {
        "help": "Name of the GenomeHubs instance.",
        "type": str,
    },
}

ID_COLUMN = {
    "flags": ["--id_column"],
    "keys": {
        "help": "Name of the column containing the record IDs.",
        "type": str,
    },
}

INDEX_TYPE = {
    "flags": ["-x", "--index_type"],
    "keys": {"help": "Type of index to fetch.", "type": str},
}

INPUT_PATH = {
    "flags": ["-i", "--input_path"],
    "keys": {"help": "Path to the input file.", "type": str},
}

MIN_VALID = {
    "flags": ["--min_valid"],
    "keys": {
        "help": "Minimum expected number of valid rows.",
        "default": 0,
        "type": int,
    },
}

MIN_ASSIGNED = {
    "flags": ["--min_assigned"],
    "keys": {
        "help": "Minimum expected number of assigned taxa.",
        "default": 0,
        "type": int,
    },
}

OUTPUT_PATH = {
    "flags": ["-o", "--output_path"],
    "keys": {"help": "Path to the output file.", "type": str},
}

QUERY_OPTIONS = {
    "flags": ["-q", "--query_options"],
    "keys": {"help": "Options for the query.", "type": str},
}

ROOT_TAXID = {
    "flags": ["-r", "--root_taxid"],
    "keys": {
        "help": "Root taxonomic ID for fetching datasets.",
        "type": str,
    },
}

S3_PATH = {
    "flags": ["-s", "--s3_path"],
    "keys": {
        "help": "Path to remote file/directory on S3.",
        "type": str,
    },
}

SSH_PATH = {
    "flags": ["--ssh_path"],
    "keys": {
        "help": "Path to remote file/directory via ssh.",
        "type": str,
    },
}

TAXDUMP_PATH = {
    "flags": ["-t", "--taxdump_path"],
    "keys": {"help": "Path to an NCBI format taxdump.", "type": str},
}

TAXON_ID = {
    "flags": ["--taxon_id"],
    "keys": {
        "help": "Taxon ID.",
        "type": str,
    },
}

TAXONOMY_NAME = {
    "flags": ["--taxonomy_name"],
    "keys": {
        "default": "ncbi",
        "help": "Name of the taxonomy.",
        "type": str,
    },
}

WORK_DIR = {
    "flags": ["-w", "--work_dir"],
    "keys": {
        "help": "Path to the working directory (default: current directory).",
        "default": ".",
        "type": str,
    },
}

YAML_PATH = {
    "flags": ["-y", "--yaml_path"],
    "keys": {"help": "Path to the source YAML file.", "type": str},
}


def default(
    arg: Dict[str, Any], default: Union[int, float, bool, str]
) -> Dict[str, Any]:
    """Return an argument with a default value."""
    # append/replace the default value to the help message
    default_re = r"\s*\(default: .*\)"
    help = re.sub(default_re, "", arg["keys"].get("help", ""))
    help += f" (default: {default})"

    return {**arg, "keys": {**arg["keys"], "help": help, "default": default}}


def multi(arg: dict) -> dict:
    """Return an argument that accepts multiple values."""
    return {**arg, "keys": {**arg["keys"], "action": "extend", "nargs": "*"}}


def required(arg: dict) -> dict:
    """Return a required argument."""
    return {**arg, "keys": {**arg["keys"], "required": True}}


def parse_args(command_line_args: list, description: str) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=description)

    for arg in command_line_args:
        parser.add_argument(*arg["flags"], **arg["keys"])

    return parser.parse_args()
