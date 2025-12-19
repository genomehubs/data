from flows.lib.utils import Parser  # noqa: E402
from flows.parsers.args import parse_args  # noqa: E402


def parse_sequencing_status(
    working_yaml: str, work_dir: str, append: bool, **kwargs
) -> None:
    """
    Wrapper function to parse the sequencing status files.

    Args:
        working_yaml (str): Path to the working YAML file.
        work_dir (str): Path to the working directory.
        append (bool): Whether to append to the existing TSV file.
        **kwargs: Additional keyword arguments.
    """
    print("parsing sequencing status files")


def plugin():
    """Register the flow."""
    return Parser(
        name="SEQUENCING_STATUS",
        func=parse_sequencing_status,
        description="Parse the sequencing status files in a directory.",
    )


if __name__ == "__main__":
    """Run the flow."""
    args = parse_args()
    parse_sequencing_status(**vars(args))
