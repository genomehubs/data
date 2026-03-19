#! /usr/bin/env python3

"""
Bulk lookup for taxon names in GoaT.

Input is a list of taxon names.

Output is a table of taxon IDs and their corresponding names.
"""

import argparse
import sys
import time
import urllib.parse

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
    parser.add_argument(
        "--follow-lineage",
        "-l",
        default="",
        help="Search along the taxonomic lineage if direct match is not found for the given field(s).",
    )
    return parser.parse_args()


def tax_lineage_search(taxon_name, goat_url, fields, ranks, follow_lineage):
    query = f"tax_lineage({taxon_name})"
    params = {
        "query": query,
        "taxonomy": "ncbi",
        "result": "taxon",
        "fields": combine_fields(fields, follow_lineage),
        "ranks": ranks,
        "size": "30",
    }
    api_url = f"{goat_url}/search?{urllib.parse.urlencode(params).replace('+', '%20')}"
    return requests.get(
        api_url, headers={"Accept": "text/tab-separated-values"}, timeout=300
    )


def add_lineage_search_rows(taxon_id, needs_lineage_search, args, outfile, headers):
    lineage_response = tax_lineage_search(
        taxon_id, args.goat_url, args.fields, args.ranks, args.follow_lineage
    )
    higher_ranks = [None] * (len(headers))
    if lineage_response.status_code == 200:
        header_fields, *rows = lineage_response.text.splitlines()
        headers = set_headers(header_fields.split("\t"), args)
        for line in rows:
            fields = line.split("\t")
            attributes = dict(zip(header_fields.split("\t"), fields))
            for field, needs_search in needs_lineage_search.items():
                if not needs_search:
                    continue
                if field not in attributes or attributes[field] in {"", "None"}:
                    continue
                agg_taxon_id = attributes.get("taxon_id")
                agg_rank = attributes.get("taxon_rank")
                rank_header = f"{field}:closest_rank"
                if rank_header in headers:
                    higher_ranks[headers.index(rank_header)] = agg_taxon_id
                id_header = f"{field}:rank_taxon_id"
                if id_header in headers:
                    higher_ranks[headers.index(id_header)] = attributes.get(field)
                rank_value_header = f"{field}:closest_value"
                if rank_value_header in headers:
                    higher_ranks[headers.index(rank_value_header)] = agg_rank
                needs_lineage_search[field] = False
    return higher_ranks


def tax_tree_search(taxon_name, goat_url, fields, ranks, follow_lineage):
    query = f"tax_tree({taxon_name}) AND tax_rank(species)"
    params = {
        "query": query,
        "taxonomy": "ncbi",
        "result": "taxon",
        "fields": combine_fields(fields, follow_lineage),
        "ranks": ranks,
    }
    api_url = f"{goat_url}/search?{urllib.parse.urlencode(params).replace('+', '%20')}"
    return requests.get(
        api_url, headers={"Accept": "text/tab-separated-values"}, timeout=300
    )


def set_headers(fields, args):
    headers = ["taxon_name"] + fields + ["match_count"]
    if args.follow_lineage:
        for field in args.follow_lineage.split(","):
            headers.extend(
                [
                    f"{field}:closest_value",
                    f"{field}:closest_rank",
                    f"{field}:rank_taxon_id",
                ]
            )
    return headers


def parse_results(taxon_name, data, args, outfile, headers):
    results = []
    for idx, line in enumerate(data.splitlines()):
        fields = line.split("\t")
        if idx == 0:
            if not headers:
                headers[:] = set_headers(fields, args)
                outfile.write("\t".join(headers) + "\n")
            continue
        if args.ancestor and args.ancestor not in fields:
            continue
        results.append(fields)
    result_count = len(results)
    rows = []
    if result_count == 0:
        rows = [[taxon_name] + ["None"] * (len(headers) - 2) + ["0"]]
    else:
        for fields in results:
            needs_lineage_search = {}
            if args.follow_lineage:
                for field in args.follow_lineage.split(","):
                    if field in headers:
                        field_idx = headers.index(field) - 1  # Adjust for taxon_name
                        needs_lineage_search[field] = fields[field_idx] in {"", "None"}
            row = (
                [taxon_name]
                + [field or "None" for field in fields]
                + [str(result_count)]
            )
            # Pad row to length of headers
            if len(row) < len(headers):
                row += ["None"] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[: len(headers)]
            # If needed, add rows for lineage search
            do_lineage_search = any(needs_lineage_search.values())
            if do_lineage_search:
                taxon_index = headers.index("taxon_id") - 1  # Adjust for taxon_name
                taxon_id = fields[taxon_index]
                extra_fields = add_lineage_search_rows(
                    taxon_id, needs_lineage_search, args, outfile, headers
                )
                for index, extra_field in enumerate(extra_fields):
                    if extra_field is not None:
                        row[index] = extra_field
            rows.append(row)
    for row in rows:
        outfile.write("\t".join(row) + "\n")


def combine_fields(fields, follow_lineage):
    if not follow_lineage:
        return fields
    field_list = fields.split(",")
    for field in follow_lineage.split(","):
        if field not in field_list:
            field_list.append(field)
    if "assembly_span" not in field_list:
        field_list.append("assembly_span")
    return ",".join(field_list)


def taxon_name_search(taxon_name, goat_url, fields, ranks, follow_lineage):
    query = f"tax_name({taxon_name})"
    params = {
        "query": query,
        "taxonomy": "ncbi",
        "result": "taxon",
        "fields": combine_fields(fields, follow_lineage),
        "ranks": ranks,
        "includeEstimates": "true",
    }
    api_url = f"{goat_url}/search?{urllib.parse.urlencode(params).replace('+', '%20')}"
    return requests.get(
        api_url, headers={"Accept": "text/tab-separated-values"}, timeout=300
    )


def main():
    args = parse_args()
    infile = open(args.input_file, "r") if args.input_file != sys.stdin else sys.stdin
    outfile = (
        open(args.output_file, "w") if args.output_file != sys.stdout else sys.stdout
    )
    headers = []
    processed = 0
    try:
        for line in infile:
            taxon_name = line.strip()
            if not taxon_name:
                continue
            print(f"Processing: {taxon_name}", file=sys.stderr)

            response = taxon_name_search(
                taxon_name, args.goat_url, args.fields, args.ranks, args.follow_lineage
            )
            if response.status_code == 200:
                parse_results(
                    taxon_name,
                    response.text,
                    args,
                    outfile,
                    headers,
                )
            else:
                print(f"Error: {response.status_code}", file=sys.stderr)

            processed += 1
            if processed % 10 == 0:
                print(f"Processed {processed} items — sleeping 1s", file=sys.stderr)
                time.sleep(1)
    finally:
        if infile is not sys.stdin:
            infile.close()
        if outfile is not sys.stdout:
            outfile.close()


if __name__ == "__main__":
    main()
