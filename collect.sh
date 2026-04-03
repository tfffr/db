#!/bin/bash

find . -name "*.go" -exec sh -c 'echo "=== FILE: {} ==="; cat "{}"; echo "\n"' \; > "$(basename "$PWD").txt"