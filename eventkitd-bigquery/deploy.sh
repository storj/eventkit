#!/usr/bin/env bash
set -euxo pipefail
CGO_ENABLED=0 go build -v -o eventkitd-bigquery .
scp eventkitd-bigquery eventkitd.datasci.storj.io:
ssh eventkitd.datasci.storj.io sudo mv eventkitd-bigquery /usr/local/bin/eventkitd-bigquery
