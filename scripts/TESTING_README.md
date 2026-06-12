# Parser Testing Wrapper

## Usage

Run all parser tests with comparison against S3 sources:

```bash
python scripts/run_parse_validate_tests.py
```

Test a specific parser:

```bash
python scripts/run_parse_validate_tests.py --parser REFSEQ_ORGANELLES
python scripts/run_parse_validate_tests.py --parser SRA_DATA
python scripts/run_parse_validate_tests.py --parser SEQUENCING_STATUS
```

Verbose output:

```bash
python scripts/run_parse_validate_tests.py --verbose
```

## What the Script Does

1. **Discovers parsers** from `flows/parsers/parse_*.py`
2. **Runs each parser** on example TSV files in `tsv_examples/`
3. **Compares output** to S3 source versions mirrored in `goat-data_s3_sources/`
4. **Validates**:
   - Header names match between parsed output and S3 source
   - Line counts are similar (within 10% tolerance)
5. **Generates a summary report** showing pass/fail status

## Test Configuration

Tests are configured in the `PARSER_INPUT_MAPPING` dict in the script:

```python
PARSER_INPUT_MAPPING = {
    "PARSER_NAME": [
        ("tsv_examples/input.tsv", "path/to/config.types.yaml", "s3_comparison_file.tsv"),
        # (input_example, yaml_config, s3_source_name)
    ],
}
```

- **input_example**: Path to example TSV in `tsv_examples/`
- **yaml_config**: Path to YAML schema (relative to data repo root)
- **s3_source_name**: Filename in `goat-data_s3_sources/` for comparison (or `None`)

## Current Test Results

### ✓ Passing (Perfect Match)

- **REFSEQ_ORGANELLES**: 33,252 rows, 13 cols - matches S3 source exactly
- **SRA_DATA**: 27,606 rows, 8 cols - matches S3 source exactly

### ✓ Partial Pass (Headers OK)

- **SEQUENCING_STATUS (vgp)**: Headers match, but example is only a sample (292 vs 1093 rows) — expected
- **SEQUENCING_STATUS (AEGIS)**: Parser succeeds, no S3 source to compare
- **BLOBTOOLKIT**: Headers mostly match (23/25 cols), example input missing 'biosample'/'bioproject' — sample is incomplete

### ❌ Known Issues

- **GENOMESIZE_KARYOTYPE**: Genomehubs `write_tsv` error with composite headers (`header: [genus, species]`). This is a YAML schema complexity issue, not a parser bug.

## Running Parsers Manually

For direct parser invocation with SKIP_PREFECT:

```bash
export SKIP_PREFECT=true
python -m flows.parsers.parse_refseq_organelles \
  -i path/to/input.tsv \
  -y path/to/config.types.yaml
```

## Adding New Tests

1. Add example TSV to `tsv_examples/` or `tsv_examples/<name>/`
2. Update `PARSER_INPUT_MAPPING` with the new test config
3. Run the script to verify

Example:

```python
"MY_PARSER": [
    ("tsv_examples/my_example.tsv", "../goat-data/sources/my-category/my_config.types.yaml", "my_source.tsv"),
]
```

## Troubleshooting

**"No output file generated"**:

- Check that the YAML config specifies a different output filename than the input
- Verify the input TSV is accessible

**"Headers mismatch"**:

- Check if the example input has all expected columns
- Verify the YAML configuration includes all fields
- Example files may be incomplete samples

**"Parser timeout"**:

- Increase the timeout in the `run_parser()` function (currently 60 seconds)
- Check for issues in the parser logic or input data

## Environment

Requires:

- `genomehubs >= 2.10.14`
- `boto3`
- `pyyaml`
- `requests`

Install with: `pip install -q -r requirements.txt`
