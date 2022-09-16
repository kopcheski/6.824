#!/bin/bash

echo "CLeaning up temporary files"
rm mr-*

echo "Building plugin wc.so"
go build -race -buildmode=plugin ../mrapps/wc.go

echo "Starting up workers"
for counter in {1..250}; do go run -race mrworker.go wc.so & go run -race mrworker.go wc.so; done