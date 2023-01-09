#!/usr/bin/env bash
CGO_ENABLED=0 go install
scp $(which eventkitd-bigquery) eventkit:
ssh eventkitd sudo mv eventkitd-bigquery /usr/local/bin/eventkitd-bigquery
