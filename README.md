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

###From the command line
`HADOOP.md` contains quick start instructions for deploying ATL/MapReduce into a Docker- or VirtualBox-based virtualized single node Hadoop cluster.

###Hadoop cluster 
It is also possible to run ATL/MapReduce on a hadoop cluster such as [CDH-Cloudera](http://www.cloudera.com/content/cloudera/en/products-and-services/cdh.html) or [Amazon Elastic MapReduce (EMR)](http://aws.amazon.com/fr/elasticmapreduce/).

##How to build

To create the ATL/MapReduce binary distribution, you will need a recent version of Eclipse (these instructions were tested with Mars). The process goes as follows:

1. Download and install a recent version of (Eclipse)[https://www.eclipse.org/downloads/] and a JDK for Java 7.
2. Go to "Window > Preferences > Java > Compiler" and change the default "Compiler compliance level" to 1.7. This is needed for the Docker image above, which comes with Java 7.
3. Go to "Help > Install New Software..." and install everything from IvyDE through its (update site)[http://www.apache.org/dist/ant/ivyde/updatesite/]. Let Eclipse restart.
4. Go to the Git perspective with "Window > Perspective > Open Perspective > Other... > Git".
5. Clone the `https://github.com/atlanmod/ATL_MR` Git repository and import its projects. This can be done by copying the URL into the clipboard, right-clicking on the "Git Repositories" view and selecting "Paste Repository Path or URI". Make sure to check the "Import all existing Eclipse projects" box on the last step of the wizard.
6. Check out the V0.1 tag of the `ATL_MR` repository by right-clicking on "ATL_MR > Tags > V0.1" and selecting "Check Out...".
7. Clone the `https://github.com/atlanmod/org.eclipse.atl.atlMR.git` repository, but do *not* import all projects. Instead, uncheck the box, let the clone finish and right click on the "plugins" folder within "Working Directory", selecting the "Import Projects..." menu entry.
8. Close all projects except for `atl-mr` and open `org.eclipse.m2m.atl.emftvm`, letting it open any referenced projects.
9. Right-click on `atl-mr` in the "Package Explorer" view and select "Export... > Ant Buildfiles".
10. Right-click on the generated `build.xml` file in the "Package Explorer" view and select "Run As > Ant Build...". Select the `dist-emftvm` configuration, and make sure in the "JRE" tab that it runs in the same JRE as the workspace.
9. Refresh the `atl-mr` project by right clicking on it in the "Package Explorer" view and selecting "Refresh".
11. Again, right-click on the generated `build.xml` file, but this time select the `dist` configuration. It does not need to run in the same JRE as the workspace, but nevertheless check in the "JRE" tab that a valid JRE has been selected.
12. Refresh the project, and we're done: the distribution will be located in `atl-mr/dist`. The files within `ivy/default` are the dependencies required to compile against Hadoop: they do not have to be redistributed, as Hadoop itself will provide them.

**Please note that hints on the execution syntax are provided. For more information please check the run.bat/run.sh files in the dist folder**.


