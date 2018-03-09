#!/bin/bash

declare -a sizeModels=( '10000' '20000' '30000' '40000' '50000' '100000' )

for model in ${!sizeModels[*]}
do
  i=${sizeModels[$model]}
  paste timings-class-$i-pass-* &> timings-class-$i-merged.txt
  cut -f1,2,4,6,8,10,12,14,16,18,20,22,24 timings-class-$i-merged.txt &> timings-class-$i-cut.txt
done
