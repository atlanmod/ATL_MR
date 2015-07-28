Quick start instructions for Hadoop
===================================

To use ATL_MR, a Hadoop cluster must be set up first.

The following sections will explain how to set up a virtualized trivial 1-node pseudo-distributed Hadoop cluster in several ways.

Please note that pseudo-distributed Hadoop clusters are only meant for quick test runs. To obtain actual performance gains, users will need to set up more realistic Hadoop clusters on their own.

Trivial cluster with Docker
---------------------------

Only a few commands are required to run a simple 1-node Hadoop cluster in a Docker container. For Ubuntu 14.04, these steps are enough:

1. Install Docker according to the (official instructions)[https://docs.docker.com/installation/ubuntulinux/].
2. Run this command to pull and run the (sequenceiq/hadoop-docker Docker image)[https://registry.hub.docker.com/u/sequenceiq/hadoop-docker/]:

    sudo docker run -it sequenceiq/hadoop-docker:2.7.0 /etc/bootstrap.sh -bash

3. Check the IP of the machine with `ip addr show eth0`. The YARN web frontend will be available on `http://IP:8088/` and the name node web frontend will be available on `http://IP:50070`.

Trivial cluster with VirtualBox
-------------------------------

Running a 1-node within VirtualBox or any other virtualization tool takes a bit more time. Here is an outline of the process:

1. Create a new Ubuntu 64-bit virtual machine, and install (Ubuntu Server 14.04.2 LTS)[http://www.ubuntu.com/download/server] on it.
2. Download a binary distribution of Hadoop from its (official page)[https://hadoop.apache.org/releases.html]. This process has been tested with 2.7.1.
3. Go to the (official installation instructions)[http://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-common/SingleCluster.html]:
   1. Follow the steps in "Prerequisites".
   2. Follow the steps in "Prepare to Start the Hadoop Cluster": please remember to edit `hadoop-env.sh`.
   3. Edit `core-site.xml` and `hdfs-site.xml` as described in "Pseudo-Distributed Operation > Configuration".
   4. Set up passwordless SSH login as described in "Pseudo-Distributed Operation > Setup passphraseless ssh".
   5. If your distribution cleans up `/tmp` between reboots (Ubuntu does), edit `hdfs-site.xml` and add this elements within the `<configuration>` element:

       <property>
         <name>dfs.namenode.name.dir</name>
         <value>/where/you/unpacked/hadoop/hdfs/namenode</value>
       </property>
       <property>
         <name>dfs.datanode.data.dir</name>
         <value>/where/you/unpacked/hadoop/hdfs/datanode</value>
       </property>

    6. Follow steps 1-4 of "Pseudo-Distributed Operation > Execution".
    7. Follow steps of "YARN on a Single Node".
4. Check the IP of the machine with `ip addr show eth0`. The YARN web frontend will be available on `http://IP:8088/` and the name node web frontend will be available on `http://IP:50070`.

Running "Families2Persons" within Docker
----------------------------------------

After creating the ATL_MR distribution as mentioned in the README.md, running the sample transformation with the above Docker instance would require these steps:

1. Run Hadoop while mounting the `atl-mr` directory into /atlmr:

    sudo docker run -v /path/to/atl-mr:/atlmr -it sequenceiq/hadoop-docker:2.7.0 /etc/bootstrap.sh -bash
    
2. In the Bash shell started by Docker, go to `/atlmr/dist` and run this command:

    bash run.sh -f ../data/Families2Persons/Families2Persons.emftvm \
      -s ../data/Families2Persons/Families.ecore \
      -t ../data/Families2Persons/Persons.ecore \
      -i ../data/Families2Persons/sample-Families.xmi

3. Retrieve the generated model from the HDFS or through "Utilities > Browse the file system" in `http://IP:50070`:

    hdfs dfs -get /user/root/data/Families2Persons/sample-Families.xmi.out.xmi output.xmi

4. The generated model will be in the `output.xmi` file of the current directory.

5. Exit and stop the container with `exit` or CTRL+d.

Running "Families2Persons" from outside Docker
----------------------------------------------

To run this transformation from outside the Docker container, you can follow these steps:

1. Create a new container based on the previous Hadoop image and log onto it:

    sudo docker create --name=hadoop-jh sequenceiq/hadoop-docker:2.7.0
    sudo docker start hadoop-jh
    sudo docker exec -it container_name bash

3. Modify the lines around the `service sshd start` line, to read as follows:

    service sshd start
    $HADOOP_PREFIX/sbin/start-dfs.sh
    $HADOOP_PREFIX/sbin/start-yarn.sh
    $HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh start historyserver

4. Commit the change into your own image:

    sudo docker commit container_name youruser/hadoopjh:latest

5. You can now start Hadoop with the usual command, but using the changed image:

    sudo docker run -it youruser/hadoopjh /etc/bootstrap.sh -bash

6. Note down the IP address for the container with `ip addr show eth0`.

7. Copy the Hadoop distribution from the container into the host:

    cd /path/to/atl-mr/dist
    sudo docker cp container_name:/usr/local/hadoop .
    sudo chown -R $(whoami): hadoop

8. Set some properties in the `hadoop/etc/hadoop` config files:

   * `core-site.xml`, `fs.defaultFS`: `hdfs://CONTAINER_IP:9000`
   * `mapred-site.xml`, `mapreduce.jobhistory.address`: `CONTAINER_IP:10020`
   * `yarn-site.xml`, `yarn.resourcemanager.hostname`: `CONTAINER_IP`

9. Run the ATL/MapReduce transformation from the host:

    bash run.sh -f ../data/Families2Persons/Families2Persons.emftvm \
      -s ../data/Families2Persons/Families.ecore \
      -t ../data/Families2Persons/Persons.ecore \
      -i ../data/Families2Persons/sample-Families.xmi
