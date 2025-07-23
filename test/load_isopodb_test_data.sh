#!/bin/bash

curl -X DELETE 'localhost:9201/*2021.10.15*'
genomehubs init \
    --config-file test/isopod_config.genomehubs.yaml \
    --taxonomy-source ncbi \
    --taxonomy-root 2759 \
    --taxon-preload &&

genomehubs index \
    --config-file test/isopod_config.genomehubs.yaml \
    --taxonomy-source ncbi \
    --taxon-dir test/taxon-data &&

genomehubs index \
    --config-file test/isopod_config.genomehubs.yaml \
    --taxonomy-source ncbi \
    --assembly-dir test/assembly-data &&

genomehubs index \
    --config-file test/isopod_config.genomehubs.yaml \
    --taxonomy-source ncbi \
    --feature-dir test/feature-data &&

genomehubs index \
    --config-file test/isopod_config.genomehubs.yaml \
    --taxonomy-source ncbi \
    --taxon-dir test/region-data &&

genomehubs fill \
    --config-file test/isopod_config.genomehubs.yaml \
    --taxonomy-source ncbi \
    --traverse-root 2759 \
    --traverse-infer-both &&

echo done ||
echo failed

kill $API_PID