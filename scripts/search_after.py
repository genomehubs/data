#!/usr/bin/env python3
import json
from urllib.parse import quote

import requests

BASE_URL = "https://goat.genomehubs.org/api/v2/searchPaginated"
OUTPUT_FILE = "assembly_results.jsonl"  # Line-delimited JSON format

# Your base query parameters
params = {
    "query": "tax_tree(2759)",
    "result": "assembly",
    "includeEstimates": True,
    "taxonomy": "ncbi",
    "includeDescendants": False,
    "emptyColumns": False,
    "limit": 1000,  # Max records per page for efficiency
}

all_results = []
search_after = None
total_fetched = 0
target_records = 50000

# Open file for writing
with open(OUTPUT_FILE, "w") as f:
    while total_fetched < target_records:
        # Add searchAfter to params if we have pagination data
        if search_after:
            params["searchAfter"] = json.dumps(search_after)

        # Make the request
        # Build encoded query string so spaces -> %20 and brackets are percent-encoded
        qs = "&".join(
            f"{quote(str(k), safe='')}={quote(str(v), safe='')}"
            for k, v in params.items()
        )
        url = f"{BASE_URL}?{qs}"
        response = requests.get(url)
        data = response.json()

        # Check if request was successful
        if not data.get("status", {}).get("success"):
            print(data)
            print(f"Error: {data.get('status', {}).get('error')}")
            break

        # Extract hits and pagination info
        hits = data.get("hits", [])
        pagination = data.get("pagination", {})

        # Write each result to file (line-delimited JSON)
        for hit in hits:
            f.write(json.dumps(hit) + "\n")

        total_fetched += len(hits)

        print(f"Fetched {total_fetched} records so far...")

        # Check if there are more results
        if not pagination.get("hasMore"):
            print(f"Reached end of results at {total_fetched} total records")
            break

        # Get the searchAfter value for the next request
        search_after = pagination.get("searchAfter")

        if not search_after:
            print("No searchAfter value returned, stopping pagination")
            break

print(f"\nTotal records written to {OUTPUT_FILE}: {total_fetched}")
