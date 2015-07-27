#ATL/MapReduce

ATL/MapReduce (ATL/MR) is a prototype tool for running complex **ATL** transformation in the cloud using **Hadoop** MapReduce.
ATL/MapReduce is implemented on top  of an extended ATL VM that can be found [here](https://github.com/atlanmod/org.eclipse.atl.atlMR/tree/master).
Coupling **ATL/MR** with the [the extended VM](https://github.com/atlanmod/org.eclipse.atl.atlMR/tree/master) has proved a good performance, especially in terms of execution time. [In our experiments](http://www.emn.fr/z-info/atlanmod/index.php/Image:Atlmr-experiments-raw-data.zip), **ATL/MR** runs up to **~6x** faster compared to the regular VM while distributing it over 8 machines.  

##How to use

The transformation configuration in ATL/MapReduce has one additional input w.r.t. the standalone one, a record file. 
This file has the job of  defining the subset of model elements to be processed by a map worker.

###Record file 

The record file contains input model elements URIs as plain string, one per line. This file will be split by hadoop in input splits. Each map worker is assigned a chunk. Usage below:

``java -jar atl-mr.jar -s <source.ecore> -i <input.xmi> [-o <records.rec>]``

  Argument                            |  Meaning
 -------------------------------------|:-----------------------------------
 -s,--source-metamodel <source.ecore> |  URI of the source metamodel file.
 -i,--input <input.xmi>               |  URI of the input file.
 -o,--output <records.rec>            |  Path of the output records file.

###Model transformation

The transformation parameters are provided by the means of arguments. Below the usage:

``yarn jar atl-mr.jar -f <transformation.emftvm> -s <source.ecore> -t <target.ecore> -r <records.rec> -i <input.xmi> [-o <output.xmi>] [-m <mappers_hint> | -n <recors_per_mapper>]  [-v | -q]``
 
  Argument                                    |  Meaning
 ---------------------------------------------|:-----------------------------------
 -f,--file <transformation.emftvm>            | URI of the ATL transformation file.       
 -s,--source-metamodel <source.ecore>         | URI of the source metamodel file.
 -t,--target-metamodel <target.ecore>         | URI of the target metamodel file.
 -r,--records <records.rec>                   | URI of the records file.
 -i,--input <input.xmi>                       | URI of the input file.
 -o,--output <output.xmi>                     | URI of the output file. Optional.
 -m,--recommended-mappers <mappers_hint>      | The recommended number of mappers (not strict, used only as a hint). Optional, defaults to 1. Excludes the use of '-n'.
 -n,--records-per-mapper <recors_per_mapper>  | Number of records to be processed by mapper. Optional, defaults to all records. Excludes the use of '-m'.
 -v,--verbose                                 | Verbose mode. Optional, disabled by default.
 -q,--quiet                                   |  Do not print any information about the transformation execution on the standard output. Optional, disabled by default.

**Please note that resource URIs with the 'hdfs://' protocol are supported**. 
##Execution modes

You can run ATL/MapReduce in two different modes, within eclipse or in a hadoop cluster.

###Within eclipse 
ATL/MapReduce can be executed within eclipse. Hadoop configuration files are already provided for Win-x86-64. 
In order to run ATL/MapReduce, please download the appropriate hadoop distribution [here](http://hadoop.apache.org/releases.html).

###Hadoop cluster 
It is also possible to run ATL/MapReduce on a hadoop cluster such as [CDH-Cloudera](http://www.cloudera.com/content/cloudera/en/products-and-services/cdh.html) or [Amazon Elastic MapReduce (EMR)](http://aws.amazon.com/fr/elasticmapreduce/).

In order to build the distribution files of the project, you must **re-create** the build.xml file by re-exporting it.
This is **necessary** to match your computer's configuration:

1. Go to *File -> Export* and select *General / Ant Buildfiles*.
2. Select  the project and press Finish.

The distribution JAR files and dependencies are automatically created and copied in the **dist** folder by executing the ``dist.emftvm`` and ``dist`` targets in the ``build.xml`` file (imported from the ``dist.xml`` ant script). The ``dist.emftvm`` targemt MUST be execute in the same JRE than the workspace (check the JRE settings in the dialog ``External Tools Configurations``).

**Please note that hints on the execution syntax are provided. For more information please check the run.bat/run.sh files in the dist folder**.


