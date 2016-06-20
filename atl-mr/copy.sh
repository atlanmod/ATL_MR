#!/bin/bash

CONTAINER=master:/root/dist/dist-trans
HOST=/home/atlanmod-17/git/hadoop-cluster-docker/hadoop-master/files/dist/dist-trans
docker cp ./dist/atl-mr.jar $CONTAINER/

docker cp ./dist/libs/atlmr/plugins/fr.inria.atlanmod.neoemf.hbase_0.1.0.atlmr.jar $CONTAINER/libs/atlmr/plugins/

cp -f ./dist/atl-mr.jar $HOST/
cp -f ./dist/libs/atlmr/plugins/fr.inria.atlanmod.neoemf.hbase_0.1.0.atlmr.jar $HOST/libs/atlmr/plugins/
