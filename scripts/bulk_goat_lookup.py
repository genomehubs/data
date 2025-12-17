#! /usr/bin/env python3

"""
Bulk lookup for taxon names in GoaT.

Input is a list of taxon names.

Output is a table of taxon IDs and their corresponding names.

Options:
  --input-file: Path to the input file containing taxon names (one per line).
  --output-file: Path to the output file for the results.
  --goat-url: Base URL for the GoaT API.
  --ranks: Taxonomic ranks to filter results.
  --ancestor: Scientific name of the ancestor to filter results.
  --fields: Comma-separated list of fields to retrieve.
"""

import argparse
import sys

import requests


def parse_args():
    parser = argparse.ArgumentParser(description="Bulk lookup for taxon names in GoaT.")
    parser.add_argument(
        "--input-file",
        "-i",
        default=sys.stdin,
        help="Path to the input file containing taxon names (one per line).",
    )
    parser.add_argument(
        "--output-file",
        "-o",
        default=sys.stdout,
        help="Path to the output file for the results.",
    )
    parser.add_argument(
        "--goat-url",
        default="https://goat.genomehubs.org/api/v2",
        help="Base URL for the GoaT API.",
    )
    parser.add_argument(
        "--ranks",
        "-r",
        default="species,family,order,class,phylum",
        help="Taxonomic ranks to retrieve.",
    )
    parser.add_argument(
        "--ancestor", "-a", help="Scientific name of the ancestor to filter results."
    )
    parser.add_argument(
        "--fields",
        "-f",
        default="none",
        help="Comma-separated list of fields to retrieve.",
    )
    return parser.parse_args()


def parse_results(taxon_name, data, ancestor, outfile, headers):
    results = []
    for idx, line in enumerate(data.splitlines()):
        fields = line.split("\t")
        if idx == 0:
            if not headers:
                headers[:] = ["taxon_name"] + fields + ["match_count"]
                outfile.write("\t".join(headers) + "\n")
            continue
        if ancestor and ancestor not in fields:
            continue
        results.append(fields)
    result_count = len(results)
    rows = []
    if result_count == 0:
        rows = [[taxon_name] + ["None"] * (len(headers) - 2) + ["0"]]
    else:
        rows.extend(
            [taxon_name] + [field or "None" for field in fields] + [str(result_count)]
            for fields in results
        )
    for row in rows:
        outfile.write("\t".join(row) + "\n")


def main():
    args = parse_args()
    infile = open(args.input_file, "r") if args.input_file != sys.stdin else sys.stdin
    outfile = (
        open(args.output_file, "w") if args.output_file != sys.stdout else sys.stdout
    )
    headers = []
    try:
        for line in infile:
            taxon_name = line.strip()
            import urllib.parse

            query = f"tax_name({taxon_name})"
            params = {
                "query": query,
                "taxonomy": "ncbi",
                "result": "taxon",
                "fields": args.fields,
                "ranks": args.ranks,
            }
            api_url = f"{args.goat_url}/search?" + urllib.parse.urlencode(
                params, safe="", quote_via=urllib.parse.quote
            )
            response = requests.get(
                api_url, headers={"Accept": "text/tab-separated-values"}, timeout=300
            )
            if response.status_code == 200:
                parse_results(
                    taxon_name, response.text, args.ancestor, outfile, headers
                )
            else:
                print(f"Error: {response.status_code}", file=sys.stderr)
    finally:
        if infile is not sys.stdin:
            infile.close()
        if outfile is not sys.stdout:
            outfile.close()


if __name__ == "__main__":
    main()
