#!/bin/bash
## Simple script to set this folder as part of PYTHONPATH
## To run it, use "source ./configure.sh"
export PYTHONPATH=$PYTHONPATH:`pwd`
echo $PYTHONPATH
