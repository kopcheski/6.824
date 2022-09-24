#!/bin/bash

echo "Cleaning up temporary files"
rm -r mr-*

echo "Building plugin wc.so"
go build -race -buildmode=plugin ../mrapps/wc.go

echo "Starting up workers"
for counter in {1..5}; do go run -race mrworker.go wc.so & echo "Worker started"; done