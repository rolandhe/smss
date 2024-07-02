#!/bin/sh
rm -fr ./out
mkdir -p ./out
go build -ldflags "-s -w"  -o ./out/smss ../