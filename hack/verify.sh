#!/bin/bash -e

echo 'Running go vet ...'
go vet ./...

echo 'Running go fmt ...'
FILES=$(gofmt -s -l pkg)

if [[ -n "${FILES}" ]]; then
    echo You have go format errors in the below files, please run "gofmt -s -w pkg"
    echo ${FILES}
    exit 1
fi

echo 'All good.'
