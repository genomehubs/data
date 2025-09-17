from enum import Enum

from shared_args import WORK_DIR, YAML_PATH, parse_args, required
from utils import enum_action

from flows.feature_parsers.register import register_plugins  # noqa: E402

PARSERS = register_plugins()


class Parser(str, Enum):
    """Enum for the parser to run."""

    # Dynamically add values from PARSERS.ParserEnum to Parser
    locals().update(PARSERS.ParserEnum.__members__)


def process_features(parser: Parser, yaml_path: str, work_dir: str):
    file_parser = PARSERS.parsers[parser.name]
    file_parser.func(working_yaml=yaml_path, work_dir=work_dir)


if __name__ == "__main__":
    """Run the flow."""
    parser = {
        "flags": ["-p", "--parser"],
        "keys": {
            "help": "Parser to use.",
            "action": enum_action(PARSERS.ParserEnum),
            "type": str,
        },
    }
    args = parse_args(
        [
            required(parser),
            required(YAML_PATH),
            WORK_DIR,
        ],
        "Fetch, parse, and validate the TSV file.",
    )
    process_features(**vars(args))
