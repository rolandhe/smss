#!/bin/sh
rm -fr ./out
mkdir -p ./out
GOOS=linux GOARCH=amd64 go build -ldflags "-s -w"  -o ./out/smss ../