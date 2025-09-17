import os
from glob import glob

from flows.lib.conditional_import import flow  # noqa: E402
from flows.lib.utils import Parser  # noqa: E402
from flows.parsers.args import parse_args  # noqa: E402


@flow(log_prints=True)
def parse_blobtoolkit_assembly_data(input_path: str, yaml_path: str):
    """
    Parse assembly data from a BlobToolKit BlobDir.

    Args:
        input_path (str): Path to the BlobToolKit BlobDir directory.
        yaml_path (str): Path to the YAML configuration file.
    """
    print(input_path)
    print(yaml_path)
    # config = utils.load_config(
    #     config_file=yaml_path,
    #     feature_file=feature_file,
    #     load_previous=append,
    # )
    # if feature_file is not None:
    #     set_up_feature_file(config)
    # biosamples = {}
    # parsed = {}
    # previous_report = {} if append else None
    # process_assembly_reports(input_path, config, biosamples, parsed, previous_report)
    # set_representative_assemblies(parsed, biosamples)
    # write_to_tsv(parsed, config)


def parse_blobtoolkit_assembly_data_wrapper(working_yaml: str, work_dir: str) -> None:
    """
    Wrapper function to parse assembly data from a BlobToolKit BlobDir.

    Args:
        working_yaml (str): Path to the working YAML file.
    """
    # use glob to find the jsonl file in the working directory
    glob_path = os.path.join(work_dir, "*.jsonl")
    paths = glob(glob_path)
    # raise error if no jsonl file is found
    if not paths:
        raise FileNotFoundError(f"No jsonl file found in {work_dir}")
    # rais error if more than one jsonl file is found
    if len(paths) > 1:
        raise ValueError(f"More than one jsonl file found in {work_dir}")
    parse_blobtoolkit_assembly_data(input_path=paths[0], yaml_path=working_yaml)


def plugin():
    """Register the flow."""
    return Parser(
        name="BLOBTOOLKIT_ASSEMBLY",
        func=parse_blobtoolkit_assembly_data_wrapper,
        description="Parse assembly data from a BlobToolKit BlobDir.",
    )


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args("Parse assembly data from a BlobToolKit BlobDir.")
    parse_blobtoolkit_assembly_data(**vars(args))
    parse_blobtoolkit_assembly_data(**vars(args))
