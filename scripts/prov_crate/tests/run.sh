#!/usr/bin/env bash
set -eoux

pip install -r ../requirements.txt
pip install roc-validator
python ../gen_crate.py data -o out

rocrate-validator -y validate -l REQUIRED -p workflow-run-crate out

rm -r out
