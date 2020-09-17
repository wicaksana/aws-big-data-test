#!/usr/bin/env bash

set -eo pipefail

rm -rf python
rm -f base_layer.zip

pip install --target ./python -r requirements.txt
zip -r base_layer.zip python