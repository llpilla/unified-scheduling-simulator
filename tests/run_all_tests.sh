#!/bin/bash

set -x

find . -name "test_*.py" | while read line; do
    python3 $line
done
