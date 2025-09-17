import argparse

from flows.lib.shared_args import INPUT_PATH, YAML_PATH
from flows.lib.shared_args import parse_args as _parse_args
from flows.lib.shared_args import required


def parse_args(description: str = "An input file parser") -> argparse.Namespace:
    """Parse command-line arguments."""
    return _parse_args([required(INPUT_PATH), required(YAML_PATH)], description)
