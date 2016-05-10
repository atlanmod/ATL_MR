#!/bin/bash

CONTAINER=master:/root/dist/dist-trans/
HOST=/home/atlanmod-17/git/hadoop-cluster-docker/hadoop-master/files/dist/dist-trans/
docker cp ./dist/atl-mr.jar $CONTAINER
cp -f ./dist/atl-mr.jar $HOST
