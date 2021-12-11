#!/bin/bash

declare -a datasets=("kpav-sd4t" "p937-wjvj" "bdjm-n7q4" "pqg4-dm6b" "bquu-z2ht" "nzjr-3966" "4d7f-74pe" "c5up-ki6j" "dsg6-ifza" "6khm-nrue" "vz8c-29aj" "ay9k-vznm" "qdq3-9eqn" "fudw-fgrp")

SPARKCODE="OriginalApproachScaled.py"

for i in "${datasets[@]}"
do
    spark-submit "$SPARKCODE" "$i"
done