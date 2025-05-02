# GenomeHubs Data

A collection of prefect flows for processing and importing data into a GenomeHubs index.


# Initial setup

## Install prefect

### Install prefect using pip

[Install prefect](https://docs.prefect.io/v3/get-started/install) in a new conda environment

```
conda create -n prefect python=3.12
conda activate prefect
pip install -U prefect
```

### Set up a prefect server

Set up a [locally hosted prefect server](https://docs.prefect.io/v3/manage/self-host). Minimal config example, use prefect cloud or postgress-backed database in production.

```
conda activate prefect
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect server start
```

### Create a `Process` work pool

[Create a work pool](https://docs.prefect.io/v3/tutorials/schedule#create-a-work-pool) on the prefect server, verify it exists and start polling for jobs

```
prefect work-pool create --type process goat-data
prefect work-pool ls
prefect worker start --pool goat-data
```

## Install dependencies

Current flows require [blobtk](), [genomehubs]() and [ncbi datasets]()

These can be installed using conda as follows

```
conda env create -f env.yaml
conda activate genomehubs_data
```

Using a separate environment works for [local development](#local-development), however further configuration may be needed to get the prefect flows to use this environment.

To install dependencies on a Mac with an Apple Silicon chip (M1, M2, etc), use `CONDA_SUBDIR=osx-64 conda env create -f env.yaml`

## Set up s3 credentials

Several flows require access to S3, access credentials can be set up with

```
mkdir -p ~/.aws
nano ~/.aws/credentials

[default]
aws_access_key_id=XXXXXXXXXXXX
aws_secret_access_key=YYYYYYYYYYYYYYYYY
aws_endpoint_url=https://cog.sanger.ac.uk
```

```
nano ~/.aws/config

[default]
endpoint_url=https://cog.sanger.ac.uk
```

## deploy flows

Example deployments are defined in `flows/prefect.yaml`. This file contains the initial steps of the goat-data pipeline. Further development of the goat-data pipeline should take place in the [goat-data repository](https://github.com/genomehubs/goat-data), leaving this as an example.

```
git clone --single-branch https://github.com/genomehubs/data
cd data
prefect --no-prompt deploy --prefect-file flows/prefect.yaml --all
```

# Local development

All example commands below assume dependencies have been installed as described in [install dependencies](#install-dependencies) above. For local development, the [install prefect](#install-prefect) and [deploy flows](#deploy-flows) steps are not needed. All flows can be run with a `SKIP_PREFECT` environment variable to run without a Prefect API connection. 

When writing new flows and tasks, please follow the established conventions, referring to existing files as examples. The import sections need to handle running with and without prefect so will typically make use of `flows/lib/conditional_import.py` and command line arguments should be standardised across all flows by importing argument definitions from `flows/lib/shared_args.py`.

Flows are broadly grouped into categories for use in different stages of the pipeline. Where there are a number of flows of a given type, these are organised into a separate subdirectory, e.g. `flows/updaters` and `flows/parsers`.

## `flows/updaters`

Updaters are flows used to update the local copy of data from a remote resource, prior to parsing. All updaters should have an output path (`-o`) to save the output to local disk and may use other arguments from `flows/lib/shared_args.py` as required.

### Example

The `update_ncbi_datasets.py` updater runs the [ncbi datasets] tool for a given root taxon ID to return a JSONL file with one line per assembly. It will optionally compare the number of lines in the fetched file to a previous version (stored in an s3 bucket) to determine whether there are additional records available. The flow emits an event on completion that can be used to trigger related flows.

```
SKIP_PREFECT=true python3 flows/updaters/update_ncbi_datasets.py -r 9608 -o /tmp/assembly-data/ncbi_datasets_canidae.jsonl -s s3://goat/resources/assembly-data/ncbi_datasets_canidae.jsonl
```

## Fetch parse validate

The flow described in `flows/lib/wrapper_fetch_parse_validate.py` is a flow-of-flows, intended to be reused for the majority of data processing ahead of a GenomeHubs import. It comprises fetch and validate subflows shared by all runs, with a pluggable subflow that can be customised to suit individual or sets of input files. This wrapper can be called directly, as with other flows, but for local development it would be more common to work on individual steps in the fetch-parse-validate sequence separately.

### Fetch

The flow at `flows/lib/fetch_previous_file_pair.py` is used to fetch a YAML/TSV file pair from a local data repository and remote S3 bucket, respectively. Both the YAML path (`-y`) and S3 path (`-s`) are required variables. The working directory (`-w`) may also be specified and defaults to the current directory. After fetching, the flow checks that headers in the TSV files match those defined in the YAML and emits an event to pass the status of this check to a parser.

#### Example

This example command assumes the [genomehubs/goat-data](https://github.com/genomehubs/goat-data) repository is available in a sibling directory.

```
SKIP_PREFECT=true python3 flows/lib/fetch_previous_file_pair.py -y ../goat-data/sources/assembly-data/ncbi_datasets_eukaryota.types.yaml -s s3://goat/sources/assembly-data -w /tmp/assembly-data
```

### `flows/parsers`

Parsers are flows used to parse a raw data file into a format ready for import into a GenomeHubs index. All parsers recieve the same set of command line arguments, allowing them to be called as part of the [fetch-parse-validate](#fetch-parse-validate) flow-of-flows. The input path (`-i`) to the raw data file and YAML path (`-y`) to the configuration file are required. An append (`-a`) flag is optional.

#### Example

The `parse_ncbi_assemblies.py` parser takes an NCBI datasets JSONL file as input and uses the YAML configuration file to define paths within each JSON object to extract data from and the headers to write these under in the output TSV. The output filename is determined by the `file.name` value in the input YAML.

This example command assumes the [genomehubs/goat-data](https://github.com/genomehubs/goat-data) repository is available in a sibling directory.

```
SKIP_PREFECT=true python3 flows/parsers/parse_ncbi_assemblies.py -i /tmp/assembly-data/ncbi_datasets_canidae.jsonl -y /tmp/assembly-data/ncbi_datasets_eukaryota.types.yaml -a
```

### Validate

The flow at `flows/lib/validate_file_pair.py` runs [blobtk validate](https://github.com/genomehubs/blobtk/wiki/blobtk-validate) to validate the entries in a TSV file against the YAML configuration and, optionally, an NCBI taxdump format taxonomy.

The `blobtk validate` command is still experimental and has not been tested on the full range of yaml tsv files that are currently considered valid by the existing import commands so please report any unexpected errors during validation as a [blobtk issue](https://github.com/genomehubs/blobtk/issues).

#### Example

```
SKIP_PREFECT=true python3 flows/lib/validate_file_pair.py -y /tmp/assembly-data/ncbi_datasets_eukaryota.types.yaml -w /tmp/assembly-data

```

## set date as variable

```
crontab -e

0 0 * * * prefect variable set --overwrite date $(date '+%Y%m%d')
```
