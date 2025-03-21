#!/usr/bin/env bash
set -eoux

pip install -r ../requirements.txt
python ../gen_crate.py data -o out

[[ -f out/predictions.cwl ]]
[[ -f out/ro-crate-metadata.json ]]
[[ -f out/tissue_high.zip ]]
[[ -f out/tumor.zip ]]
cmp <(cat expected_output/ro-crate-metadata.json | jq  'del(."@graph"[0].datePublished, ."@graph"[0].mentions, ."@graph"[7].object, ."@graph"[7].result) | del(."@graph"[]."@id")') <(cat out/ro-crate-metadata.json | jq  'del( ."@graph"[0].datePublished, ."@graph"[0].mentions,."@graph"[7].object, ."@graph"[7].result) | del(."@graph"[]."@id")')

rm -r out

