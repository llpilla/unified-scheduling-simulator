#!/bin/bash

set -x
cd tests

find . -name "test_*.py" | while read line; do
    python3 $line
done
