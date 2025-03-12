#!/usr/bin/env python3

# sourcery skip: avoid-builtin-shadow
import argparse
import sys
from os.path import abspath, dirname

if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
    __package__ = "flows"

from flows.lib.shared_args import INPUT_PATH, YAML_PATH
from flows.lib.shared_args import parse_args as _parse_args
from flows.lib.shared_args import required


def parse_args(description: str = "An input file parser") -> argparse.Namespace:
    """Parse command-line arguments."""
    return _parse_args([required(INPUT_PATH), required(YAML_PATH)], description)
