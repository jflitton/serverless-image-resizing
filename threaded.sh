#!/bin/bash
CORES=$(grep -c ^processor /proc/cpuinfo)

for i in `seq 1 $CORES`;
    do
            python asynchronous.py &
    done