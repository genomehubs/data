#!/usr/bin/env python3

from typing import Generator

from conditional_import import flow
from shared_args import INPUT_PATH, S3_PATH, WORK_DIR, multi, parse_args, required


def iterate_records(input_path: str, work_dir: str) -> Generator[dict, None, None]:
    """
    Iterate over the records.

    Args:
        input_path (str): Path to the input file.
        work_dir (str): Path to the working directory.
    """
    with open(input_path, "r") as file:
        headers = file.readline().strip().split("\t")
        for line in file:
            fields = line.strip().split("\t")
            yield dict(zip(headers, fields))


@flow()
def for_each_record(input_path: str, work_dir: str, s3_path: list) -> None:
    for record in iterate_records(input_path, work_dir):
        print(record)


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [required(INPUT_PATH), WORK_DIR, multi(S3_PATH)],
        "Run a flow for each record in an input file.",
    )

    # Run the flow
    for_each_record(**vars(args))
