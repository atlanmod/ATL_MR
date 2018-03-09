#!/bin/bash

declare -a sizeModels=( '10000' '20000' '30000' '40000' '50000' '100000')

for model in ${!sizeModels[*]}
do
  i=${sizeModels[$model]}
  cat timings-class-$i-cut.txt | awk '{ sum=0; n=0; for(i=2;i<=NF;i++) { sum+=$i; ++n } print $1,"\t", int(sum/n) }' &> timings-class-$i-average.txt
done
