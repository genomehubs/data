from typing import Generator

from conditional_import import flow
from shared_args import (
    ID_COLUMN,
    INPUT_PATH,
    S3_PATH,
    SSH_PATH,
    WORK_DIR,
    multi,
    parse_args,
    required,
)


def iterate_records(input_path: str, id_column: str) -> Generator[dict, None, None]:
    """
    Iterate over the records.

    Args:
        input_path (str): Path to the input file.
        id_column (str): Name of the column containing the record IDs.
    """
    with open(input_path, "r") as file:
        headers = file.readline().strip().split("\t")
        if id_column not in headers:
            raise ValueError(f"Column '{id_column}' not found in headers")
        for line in file:
            fields = line.strip().split("\t")
            yield dict(zip(headers, fields))


@flow()
def for_each_record(
    input_path: str, id_column: str, work_dir: str, s3_path: list
) -> None:
    for record in iterate_records(input_path, id_column):
        print(record[id_column])


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args(
        [
            required(INPUT_PATH),
            required(ID_COLUMN),
            WORK_DIR,
            multi(S3_PATH),
            # multi(SSH_PATH),
        ],
        "Run a flow for each record in an input file.",
    )

    # Run the flow
    for_each_record(**vars(args))
