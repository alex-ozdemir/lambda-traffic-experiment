#!/usr/bin/zsh
aws s3 cp s3://rust-test-2/receiver-results-$1.csv  ./data/
aws s3 cp s3://rust-test-2/sender-results-$1.csv  ./data/
