#!/usr/bin/python3

import contextlib
import gzip
import hashlib
import os
import re
import shutil
import subprocess
from argparse import Action
from csv import DictReader, Sniffer
from datetime import datetime
from io import StringIO
from shlex import shlex
from typing import Dict, List, Optional

import boto3
import requests
from botocore.exceptions import ClientError
from dateutil import parser
from genomehubs import utils as gh_utils


def set_feature_headers() -> list[str]:
    """Set chromosome headers.

    Returns:
        list: The list of headers.
    """
    return [
        "assembly_id",
        "sequence_id",
        "start",
        "end",
        "strand",
        "length",
        "midpoint",
        "midpoint_proportion",
        "seq_proportion",
    ]


class Config:
    """
    Configuration class for Genomehubs YAML parsing.
    """

    def __init__(self, config_file, feature_file=None, load_previous=False):
        """
        Initialize the configuration class.

        Args:
            config_file (str): Path to the YAML configuration file.
            feature_file (str): Path to the feature file.

        Returns:
            Config: The configuration class.
        """
        self.config = gh_utils.load_yaml(config_file)
        self.meta = gh_utils.get_metadata(self.config, config_file)
        self.headers = gh_utils.set_headers(self.config)
        self.parse_fns = gh_utils.get_parse_functions(self.config)
        self.previous_parsed = {}
        if load_previous:
            with contextlib.suppress(Exception):
                self.previous_parsed = gh_utils.load_previous(
                    self.meta["file_name"], "genbankAccession", self.headers
                )
        self.feature_file = feature_file
        if feature_file is not None:
            self.feature_headers = set_feature_headers()
            try:
                self.previous_features = gh_utils.load_previous(
                    feature_file, "assembly_id", self.feature_headers
                )
            except Exception:
                self.previous_features = {}


def load_config(
    config_file: str, feature_file: Optional[str] = None, load_previous: bool = False
) -> Config:
    """
    Load the configuration file.

    Args:
        config_file (str): Path to the YAML configuration file.
        feature_file (str): Path to the feature file.
        load_previous (bool): Whether to load the previous data.

    Returns:
        Config: The configuration class.
    """
    return Config(config_file, feature_file, load_previous)


class Parser:
    """
    Parser class for data file parsing.
    """

    def __init__(self, name: str, func: callable, description: str):
        """
        Initialize the parser class.

        Args:
            name (str): The name of the parser.
            func (callable): The parsing function.
            description (str): The description of the parser.

        Returns:
            Parser: The parser class.
        """
        self.name = name
        self.func = func
        self.description = description

    def parse(self, data: dict) -> dict:
        """
        Parse the data dictionary.

        Args:
            data (dict): The data dictionary to parse.

        Returns:
            dict: The parsed data dictionary.
        """
        parsed_data = {}
        self.callback(data, parsed_data)
        return parsed_data


def format_entry(entry, key: str, meta: dict) -> str:
    """
    Formats a single entry in a dictionary, handling the case where the entry is a list.

    Args:
        entry (Union[str, list]): The entry to be formatted, which may be a single
            value or a list of values.
        key (str): The key associated with the entry.
        meta (dict): A dictionary containing metadata, including a "separators"
            dictionary that maps keys to separator strings.

    Returns:
        str: The formatted entry, where list elements are joined using the separator
            specified in the "separators" dictionary.
    """
    if not isinstance(entry, list):
        return str(entry)
    if "separators" not in meta or isinstance(meta["separators"], str):
        return ",".join([str(e) for e in entry if e is not None])
    return (
        meta["separators"].get(key, ",").join([str(e) for e in entry if e is not None])
    )


def append_to_tsv(headers: list[str], rows: list[dict], meta: dict):
    """
    Appends the provided rows to a TSV file with the specified file name.

    Args:
        headers (list[str]): A list of column headers.
        rows (list[dict]): A list of dictionaries, where each dictionary represents a
            row of data and the keys correspond to the column headers.
        meta (dict): A dictionary containing metadata, including the "file_name" key
            which specifies the output file name.
    """
    with open(meta["file_name"], "a") as f:
        for row in rows:
            if isinstance(row, dict):
                f.write(
                    "\t".join(
                        [format_entry(row.get(col, []), col, meta) for col in headers]
                    )
                    + "\n"
                )


def convert_keys_to_camel_case(data: dict) -> dict:
    """
    Recursively converts all keys in a dictionary to camel case.

    Args:
        data (dict): The dictionary to convert.

    Returns:
        dict: The dictionary with keys converted to camel case.
    """
    converted_data = {}
    if isinstance(data, list):
        return [convert_keys_to_camel_case(item) for item in data]
    elif not isinstance(data, dict):
        return data
    for key, value in data.items():
        if isinstance(value, (dict, list)):
            value = convert_keys_to_camel_case(value)
        converted_key = "".join(
            word.capitalize() if i > 0 else word
            for i, word in enumerate(key.split("_"))
        )
        converted_data[converted_key] = value
    return converted_data


def set_organelle_name(seq: dict) -> Optional[str]:
    """
    Determines the organelle type (mitochondrion or plastid) based on the assigned
    molecule location type in the provided sequence data.

    Args:
        seq (dict): A dictionary containing sequence data, including the
            "assigned_molecule_location_type" field.

    Returns:
        Optional[str]: The organelle type, either "mitochondrion" or "plastid", or None
            if key error.
    """
    try:
        return (
            "mitochondrion"
            if seq[0]["assigned_molecule_location_type"].casefold()
            == "Mitochondrion".casefold()
            else "plastid"
        )
    except KeyError:
        return None


def is_assembled_molecule(seq: dict) -> bool:
    """
    Determines if the provided sequence data represents an assembled molecule.

    Args:
        seq (dict): A dictionary containing sequence data, including the "role" field.

    Returns:
        bool: True if the sequence data represents an assembled molecule, False
            otherwise.
    """
    try:
        return seq[0]["role"] == "assembled-molecule"
    except (IndexError, KeyError):
        return False


def set_additional_organelle_values(
    seq: dict, organelle: dict, data: dict, organelle_name: str
) -> None:
    """
    Sets additional organelle-related values in the provided data dictionary based on
    the sequence data. If the sequence data represents an assembled molecule, it
    extracts the GenBank accession, total sequence length, and GC percentage, and
    stores these values in the `processedOrganelleInfo` dictionary. If the sequence
    data does not represent an assembled molecule, it stores the accession numbers of
    the individual scaffolds in the `processedOrganelleInfo` dictionary.

    Args:
        seq (dict): A dictionary containing sequence data.
        organelle (dict): A dictionary representing an organelle.
        data (dict): A dictionary containing processed data.
        organelle_name (str): The name of the organelle.
    """
    if is_assembled_molecule(seq):
        if "genbank_accession" in seq[0]:
            organelle["genbankAssmAccession"] = seq[0]["genbank_accession"]
            organelle["totalSequenceLength"] = seq[0]["length"]
            organelle["gcPercent"] = seq[0]["gc_percent"]
            data["processedOrganelleInfo"][organelle_name]["assemblySpan"] = organelle[
                "totalSequenceLength"
            ]
            data["processedOrganelleInfo"][organelle_name]["gcPercent"] = organelle[
                "gcPercent"
            ]
            data["processedOrganelleInfo"][organelle_name]["accession"] = seq[0][
                "genbank_accession"
            ]
    else:
        data["processedOrganelleInfo"][organelle_name]["scaffolds"] = ";".join(
            [
                entry["genbank_accession"]
                for entry in seq
                if "genbank_accession" in entry
            ]
        )


def initialise_organelle_info(data: dict, organelle_name: str):
    """
    Initializes the `processedOrganelleInfo` dictionary in the provided `data`
    dictionary, creating a new entry for the specified `organelle_name` if it doesn't
    already exist.

    Args:
        data (dict): The dictionary containing the processed data.
        organelle_name (str): The name of the organelle.
    """
    if "processedOrganelleInfo" not in data:
        data["processedOrganelleInfo"] = {}
    if organelle_name not in data["processedOrganelleInfo"]:
        data["processedOrganelleInfo"][organelle_name] = {}


def set_organelle_values(data: dict, seq: dict) -> dict:
    """
    Sets organelle-related values in the provided data dictionary based on the sequence
    report data.

    Args:
        data (dict): A dictionary containing processed data.
        seq (dict): A dictionary containing sequence data.

    Returns:
        dict: A dictionary containing organelle-related information.
    """
    organelle: dict = {
        "sourceAccession": data["accession"],
        "organelle": seq[0]["assigned_molecule_location_type"],
    }
    organelle_name: str = set_organelle_name(seq)
    initialise_organelle_info(data, organelle_name)
    set_additional_organelle_values(seq, organelle, data, organelle_name)
    return organelle


def add_organelle_entries(data: dict, organelles: dict) -> None:
    """
    Adds entries for co-assembled organelles to the provided data dictionary.

    Args:
        data (dict): A dictionary containing processed data.
        organelles (dict): A dictionary containing sequence data for co-assembled
            organelles.

    Returns:
        None
    """
    if not organelles:
        return
    data["organelles"] = []
    for seq in organelles.values():
        try:
            organelle = set_organelle_values(data, seq)
            data["organelles"].append(organelle)
        except Exception as err:
            print("ERROR: ", err)
            raise err


def add_chromosome_entries(data: dict, chromosomes: list[dict]) -> None:
    """
    Adds feature entries for assembled chromosomes to the provided data object.

    Args:
        data (dict): A dictionary containing processed data.
        chromosomes (list): A list of dictionaries containing sequence data for
            assembled chromosomes.

    Returns:
        None
    """
    data["chromosomes"] = []
    for seq in chromosomes:
        data["chromosomes"].append(
            {
                "assembly_id": data["processedAssemblyInfo"]["genbankAccession"],
                "sequence_id": seq.get("genbank_accession", ""),
                "start": 1,
                "end": seq["length"],
                "strand": 1,
                "length": seq["length"],
                "midpoint": round(seq["length"] / 2),
                "midpoint_proportion": 0.5,
                "seq_proportion": seq["length"]
                / int(data["assemblyStats"]["totalSequenceLength"]),
            }
        )


def is_chromosome(seq: dict) -> bool:
    """
    Determines if the given sequence data represents an assembled chromosome.

    Args:
        seq (dict): A dictionary containing sequence data.

    Returns:
        bool: True if the sequence data represents an assembled chromosome, False
            otherwise.
    """
    return seq["role"] == "assembled-molecule"


def is_non_nuclear(seq: dict) -> bool:
    """
    Determines if the given sequence data represents a non-nuclear sequence.

    Args:
        seq (dict): A dictionary containing sequence data.

    Returns:
        bool: True if the sequence data represents a non-nuclear sequence, False
            otherwise.
    """
    return seq["assembly_unit"] == "non-nuclear"


def is_assigned_to_chromosome(seq: dict) -> bool:
    """
    Determines if the given sequence data represents a sequence that is assigned to a
    chromosome.

    Args:
        seq (dict): A dictionary containing sequence data.

    Returns:
        bool: True if the sequence data represents a sequence that is assigned to a
            chromosome, False otherwise.
    """
    return (
        seq["assembly_unit"] == "Primary Assembly"
        and seq["assigned_molecule_location_type"]
        in [
            "Chromosome",
            "Linkage Group",
        ]
        and seq["role"] in ["assembled-molecule", "unlocalized-scaffold"]
    )


def check_ebp_criteria(
    data: dict, span: int, chromosomes: list, assigned_span: int
) -> bool:
    """
    Checks if the given assembly data meets the EBP (Earth BioGenome Project) criteria.

    Args:
        data (dict): A dictionary containing assembly statistics and information.
        span (int): The total span of the assembly.
        chromosomes (list): A list of chromosome names.
        assigned_span (int): The total span of the sequences assigned to chromosomes.

    Returns:
        None: This function modifies the `data` dictionary in-place to add the processed
            assembly statistics.
    """
    contig_n50 = int(data["assemblyStats"].get("contigN50", 0))
    scaffold_n50 = int(data["assemblyStats"].get("scaffoldN50", 0))
    assignedProportion = None
    if not chromosomes:
        return False
    data["processedAssemblyStats"] = {}
    assignedProportion = assigned_span / span
    standardCriteria = []
    if contig_n50 >= 1000000 and scaffold_n50 >= 10000000:
        standardCriteria.append("6.7")
    if assignedProportion >= 0.9:
        if contig_n50 >= 1000000:
            standardCriteria.append("6.C")
        elif scaffold_n50 < 1000000 and contig_n50 >= 100000:
            standardCriteria.append("5.C")
        elif scaffold_n50 < 10000000 and contig_n50 >= 100000:
            standardCriteria.append("5.6")
    if standardCriteria:
        data["processedAssemblyStats"]["ebpStandardDate"] = data["assemblyInfo"][
            "releaseDate"
        ]
        data["processedAssemblyStats"]["ebpStandardCriteria"] = standardCriteria
    data["processedAssemblyStats"]["assignedProportion"] = assignedProportion
    return False


def update_organelle_info(data: dict, row: dict) -> None:
    """Update organelle info in data dict with fields from row.

    Args:
        data (dict): A dictionary containing the organelle information.
        row (dict): A dictionary containing the fields to update the organelle
        information with.

    Returns:
        None: This function modifies the `data` dictionary in-place.
    """
    for organelle in data.get("organelles", []):
        organelle.update(
            {
                k: row[k]
                for k in row.keys()
                & {
                    "taxId",
                    "organismName",
                    "commonName",
                    "releaseDate",
                    "submitter",
                    "bioProjectAccession",
                    "biosampleAccession",
                }
            }
        )


def enum_action(enum_class):
    """Creates an argparse action for handling enum values.

    Creates a custom argparse action that allows command-line arguments to be
    interpreted as members of a given enumeration class. The action converts
    case-insensitive input values to the corresponding enum members.

    Args:
        enum_class (type): The enumeration class.

    Returns:
        EnumAction: The custom argparse action.
    """

    class EnumAction(Action):
        """
        Custom argparse action for handling enum values.
        """

        def __init__(self, *args, **kwargs):
            """
            Initialize the EnumAction.

            Args:
                *args: Variable length argument list.
                **kwargs: Arbitrary keyword arguments.
            """
            table = {member.name.casefold(): member for member in enum_class}
            super().__init__(
                *args,
                choices=table,
                **kwargs,
            )
            print(enum_class)
            self.table = table

        def __call__(self, parser, namespace, values, option_string=None):
            """
            Set the corresponding enum member in the namespace.

            Args:
                parser: The argparse parser.
                namespace: The argparse namespace.
                values: The value provided.
                option_string: The option string.
            """
            setattr(namespace, self.dest, self.table[values])

    return EnumAction


def safe_get(*args, method="GET", timeout=300, **kwargs):
    if method == "GET":
        return requests.get(*args, timeout=timeout, **kwargs)
    elif method == "POST":
        return requests.post(*args, timeout=timeout, **kwargs)
    elif method == "HEAD":
        return requests.head(*args, timeout=timeout, **kwargs)


def find_http_file(http_path: str, filename: str) -> str:
    """
    Find files for the record ID.

    Args:
        http_path (str): Path to the HTTP directory.
        filename (str): Name of the file to find.

    Returns:
        str: Path to the file.
    """
    response = safe_get(f"{http_path}/{filename}")
    return f"{http_path}/{filename}" if response.status_code == 200 else None


def get_genomehubs_attribute_value(result: dict, attribute: str) -> str:
    """
    Get the value of an attribute from the result.

    Args:
        result (dict): Result.
        attribute (str): Attribute to get the value of.

    Returns:
        str: Value of the attribute.
    """
    return (
        result.get("result", {})
        .get("fields", {})
        .get("odb10_lineage", {})
        .get("value", None)
    )


def find_s3_file(s3_path: list, filename: str) -> str:
    """
    Find files for the record ID.

    Args:
        s3_path (list): List of paths to the S3 buckets.
        filename (str): Name of the file to find.

    Returns:
        str: Path to the file.
    """
    for s3_bucket in s3_path:
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=filename,
        )
        if "Contents" in response:
            return f"s3://{s3_bucket}/{response['Contents'][0]['Key']}"
    return None


def is_safe_path(path: str) -> bool:
    # Only allow alphanumeric, dash, underscore, dot, slash, colon (for s3), tilde,
    # and absolute paths.
    # Tilde (~) and absolute paths are allowed because this function is only used
    # with trusted internal input.
    # Directory traversal ('..') is blocked.
    # Allow URLs (e.g., http://, https://, s3://) and URL-safe characters
    # URL-safe: alphanumeric, dash, underscore, dot, slash, colon, tilde, percent,
    #           question, ampersand, equals
    url_pattern = r"^[\w]+://"
    url_safe_pattern = r"^[\w\-/.:~%?&=]+$"
    if re.match(url_pattern, path):
        return ".." not in path and re.match(url_safe_pattern, path)
    return ".." not in path if re.match(url_safe_pattern, path) else False


def run_quoted(cmd, **kwargs):
    quoted_cmd = [shlex.quote(str(arg)) for arg in cmd]
    return subprocess.run(quoted_cmd, **kwargs)


def popen_quoted(cmd, **kwargs):
    quoted_cmd = [shlex.quote(str(arg)) for arg in cmd]
    return subprocess.Popen(quoted_cmd, **kwargs)


def parse_s3_path(s3_path):
    # Extract bucket name and key from the S3 path
    bucket, key = s3_path.removeprefix("s3://").split("/", 1)
    return bucket, key


def fetch_from_s3(s3_path: str, local_path: str, gz: bool = False) -> None:
    """
    Fetch a file from S3.

    Args:
        s3_path (str): Path to the remote file on s3.
        local_path (str): Path to the local file.
        gz (bool): Whether to gunzip the file after downloading. Defaults to False.

    Returns:
        None: This function downloads the file from S3 to the local path.
    """
    s3 = boto3.client("s3")

    bucket, key = parse_s3_path(s3_path)

    if s3_path.endswith(".gz") and not local_path.endswith(".gz"):
        gz = True

    # Download the file from S3 to the local path
    try:
        if gz:
            gz_path = f"{local_path}.gz"
            s3.download_file(Bucket=bucket, Key=key, Filename=gz_path)
            # Unzip gz_path to local_path, then remove gz_path
            with gzip.open(gz_path, "rb") as f_in, open(local_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(gz_path)
        else:
            s3.download_file(Bucket=bucket, Key=key, Filename=local_path)
    except ClientError as e:
        print(f"Error downloading {s3_path} to {local_path}: {e}")
        raise e


def upload_to_s3(local_path: str, s3_path: str, gz: bool = False) -> None:
    """
    Upload a file to S3.

    Args:
        local_path (str): Path to the local file.
        s3_path (str): Path to the remote file on s3.
        gz (bool): Whether to gzip the file before uploading. Defaults to False.

    Returns:
        None: This function uploads the local file to S3.
    """

    if not is_safe_path(local_path):
        raise ValueError(f"Unsafe local path: {local_path}")
    if not is_safe_path(s3_path):
        raise ValueError(f"Unsafe s3 path: {s3_path}")

    if s3_path.endswith(".gz") and not local_path.endswith(".gz"):
        gz = True

    try:
        if gz:
            gz_path = f"{local_path}.gz"
            with open(local_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                f_out.write(f_in.read())
            try:
                # use s3cmd for uploads due to issues with boto3 and large files
                cmd = [
                    "s3cmd",
                    "put",
                    "--acl-public",
                    gz_path,
                    s3_path,
                ]
                result = run_quoted(
                    cmd,
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    print(
                        (
                            f"Error uploading {local_path} to {s3_path} "
                            f"with s3cmd: {result.stderr}"
                        )
                    )
                    raise RuntimeError(f"s3cmd upload failed: {result.stderr}")
            finally:
                if os.path.exists(gz_path):
                    os.remove(gz_path)
        else:
            # use s3cmd for uploads due to issues with boto3 and large files
            cmd = [
                "s3cmd",
                "put",
                "--acl-public",
                local_path,
                s3_path,
            ]
            result = run_quoted(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(
                    (
                        f"Error uploading {local_path} to {s3_path} "
                        f"with s3cmd: {result.stderr}"
                    )
                )
                raise RuntimeError(f"s3cmd upload failed: {result.stderr}")
    except Exception as e:
        print(f"Error uploading {local_path} to {s3_path}: {e}")
        raise e


def set_index_name(
    index_type: str,
    hub_name: str,
    taxonomy_name: str = "ncbi",
    date: str = None,
    separator: str = "--",
) -> str:
    """
    Set the index name.

    Args:
        index_type (str): Type of index.
        hub_name (str): Name of the GenomeHubs instance.
        taxonomy_name (str, optional): Name of the taxonomy. Defaults to "ncbi".
        date (str, optional): Date of the index. Defaults to None.
        separator (str, optional): Separator for the index name. Defaults to "--".

    Returns:
        str: Name of the index.
    """
    if date is None:
        # set todays date in the format YYYY.MM.DD
        date = datetime.now().strftime("%Y.%m.%d")
    else:
        # change the date format to YYYY.MM.DD
        date = date.replace("-", ".")
    return (
        f"{hub_name}{separator}{taxonomy_name}{separator}{index_type}{separator}{date}"
    )


def parse_tsv(text: str) -> List[Dict[str, str]]:
    """
    Parse a TSV string into a list of dictionaries.

    Args:
        text (str): The TSV string to parse.

    Returns:
        List[Dict[str, str]]: A list of dictionaries representing the rows.
    """
    sniffer = Sniffer()
    dialect = sniffer.sniff(text)
    reader = DictReader(StringIO(text), dialect=dialect)
    return list(reader)


def last_modified_git_remote(http_path: str) -> Optional[int]:
    """
    Get the last modified date of a file in a git repository.

    Args:
        http_path (str): Path to the HTTP file.

    Returns:
        Optional[int]: Last modified date of the file, or None if not found.
    """
    try:
        if not http_path.startswith("https://gitlab.com/"):
            print(f"Malformed GitLab URL (missing prefix): {http_path}")
            return None
        project_path = http_path.removeprefix("https://gitlab.com/")
        project_path = project_path.removesuffix(".git")
        parts = project_path.split("/")
        if len(parts) < 6:
            print(f"Malformed GitLab URL (not enough parts): {http_path}")
            return None
        project = "%2F".join(parts[:2])
        ref = parts[4]
        file = "%2F".join(parts[5:]).split("?")[0]
        api_url = (
            f"https://gitlab.com/api/v4/projects/{project}/repository/commits"
            f"?ref_name={ref}&path={file}&per_page=1"
        )
        response = safe_get(api_url)
        if response.status_code == 200:
            commits = response.json()
            if commits and commits[0].get("committed_date"):
                dt = parser.isoparse(commits[0]["committed_date"])
                return int(dt.timestamp())
        else:
            response = safe_get(http_path, method="HEAD", allow_redirects=True)
            if response.status_code == 200:
                if last_modified := response.headers.get("Last-Modified", None):
                    dt = parser.parse(last_modified)
                    return int(dt.timestamp())
                return None
    except Exception as e:
        print(f"Error parsing GitLab URL or fetching commit info: {e}")
    return None


def last_modified_http(http_path: str) -> Optional[int]:
    """
    Get the last modified date of a file.

    Args:
        http_path (str): Path to the HTTP file.

    Returns:
        Optional[int]: Last modified date of the file, or None if not found.
    """
    if "gitlab.com" in http_path:
        return last_modified_git_remote(http_path)
    response = safe_get(http_path, method="HEAD", allow_redirects=True)
    if response.status_code == 200:
        if last_modified := response.headers.get("Last-Modified", None):
            dt = parser.parse(last_modified)
            return int(dt.timestamp())
        return None
    return None


def last_modified_s3(s3_path: str) -> Optional[int]:
    """
    Get the last modified date of a file on S3.

    Args:
        s3_path (str): Path to the remote file on s3.

    Returns:
        Optional[int]: Last modified date of the file, or None if not found.
    """
    s3 = boto3.client("s3")

    bucket, key = parse_s3_path(s3_path)

    # Return None if the remote file does not exist
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        last_modified = response.get("LastModified", None)
        return int(last_modified.timestamp()) if last_modified is not None else None
    except ClientError:
        return None


def last_modified(local_path: str) -> Optional[int]:
    """
    Get the last modified date of a local file.

    Args:
        local_path (str): Path to the local file.

    Returns:
        Optional[int]: Last modified date of the file as a Unix timestamp, or None if
            the file does not exist.
    """
    if not os.path.exists(local_path):
        return None
    mtime = os.path.getmtime(local_path)
    return int(mtime)


def is_local_file_current_http(local_path: str, http_path: str) -> bool:
    """
    Compare the last modified date of a local file with a remote file on HTTP.

    Args:
        local_path (str): Path to the local file.
        http_path (str): Path to the HTTP directory.

    Returns:
        bool: True if the local file is up-to-date, False otherwise.
    """
    local_date = last_modified(local_path)
    remote_date = last_modified_http(http_path)
    print(f"Local date: {local_date}, Remote date: {remote_date}")
    if local_date is None or remote_date is None:
        return False
    return local_date >= remote_date


def generate_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
