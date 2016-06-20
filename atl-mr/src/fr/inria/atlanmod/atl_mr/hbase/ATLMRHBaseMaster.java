package fr.inria.atlanmod.atl_mr.hbase;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import jline.Terminal;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.emf.common.util.URI;

import fr.inria.atlanmod.atl_mr.utils.ATLMRTableInputFormat2;
import fr.inria.atlanmod.neoemf.util.NeoEMFUtil;

public class ATLMRHBaseMaster extends Configured implements Tool {

	protected static final String JOB_NAME = "ATL in Hadoop/HBase";

	protected static final int STATUS_OK = 0;
	protected static final int STATUS_ERROR = 1;

	static final String TRANSFORMATION 						= "f";
	static final String SOURCE_PACKAGE 						= "s";
	static final String TARGET_PACKAGE 						= "t";
	static final String INPUT_MODEL 						= "i";
	static final String OUTPUT_MODEL 						= "o";

	public static final String RECOMMENDED_MAPPERS 			= "m";
	private static final String RECORDS_PER_MAPPER	 		= "n";
	private static final String QUIET 						= "q";
	private static final String VERBOSE 					= "v";
	private static final String COUNTERS 					= "c";

	private static final String TRANSFORMATION_LONG 		= "file";
	private static final String SOURCE_PACKAGE_LONG 		= "source-package";
	private static final String TARGET_PACKAGE_LONG 		= "target-package";
	private static final String INPUT_MODEL_LONG 			= "input";
	private static final String OUTPUT_MODEL_LONG 			= "output";
	private static final String RECOMMENDED_MAPPERS_LONG	= "recommended-mappers";
	private static final String RECORDS_PER_MAPPER_LONG	 	= "records-per-mapper";
	private static final String QUIET_LONG 					= "quiet";
	private static final String VERBOSE_LONG 				= "verbose";
	private static final String COUNTERS_LONG 				= "counters";


	private static String inModel;

	private static class OptionComarator<T extends Option> implements Comparator<T> {
		private static final String OPTS_ORDER = "fstriomnvq";

		@Override
		public int compare(T o1, T o2) {
			return OPTS_ORDER.indexOf(o1.getOpt()) - OPTS_ORDER.indexOf(o2.getOpt());
		}
	}

	/**
	 * Main program, delegates to ToolRunner
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Launch the transformation
		int res = ToolRunner.run(conf, new ATLMRHBaseMaster(), args);


		// cleanUp the transformation environment

		Logger.getGlobal().log(Level.INFO, "Cleaning up after job finish - START");

		NeoEMFUtil.ResourceUtil.INSTANCE.deleteResourceIfExists(URI.createURI(inModel).appendSegment(ATLMapReduceTask.TRACES_NSURI));
		NeoEMFUtil.ResourceUtil.INSTANCE.deleteResourceIfExists(URI.createURI(inModel).appendSegment("traces").appendSegment("map"));

		Logger.getGlobal().log(Level.INFO, "Cleaning up reducer - END");

		System.exit(res);
	}

	/**
	 * Hadoop {@link Tool} implementation
	 */
	@Override
	public int run(String[] args) throws Exception {

		Options options = new Options();

		configureOptions(options);

		CommandLineParser parser = new GnuParser();

		try {
			CommandLine commandLine = parser.parse(options, args);

			if (commandLine.hasOption(VERBOSE)) {
				Logger.getGlobal().setLevel(Level.FINEST);
			}

			if (commandLine.hasOption(QUIET)) {
				Logger.getGlobal().setLevel(Level.OFF);
			}

			String transformationLocation = commandLine.getOptionValue(TRANSFORMATION);
			String sourcemmLocation = commandLine.getOptionValue(SOURCE_PACKAGE);
			String targetmmLocation = commandLine.getOptionValue(TARGET_PACKAGE);
			//String recordsLocation = commandLine.getOptionValue(RECORDS_FILE);
			String inputLocation = commandLine.getOptionValue(INPUT_MODEL);
			// dirty just for test
			inModel = inputLocation;
			String outputLocation = commandLine.getOptionValue(OUTPUT_MODEL);

			int recommendedMappers = 1;
			if (commandLine.hasOption(RECOMMENDED_MAPPERS)) {
				recommendedMappers = ((Number) commandLine.getParsedOptionValue(RECOMMENDED_MAPPERS)).intValue();
			}
			boolean counters = false;
			if (commandLine.hasOption(COUNTERS)) {
				counters = true;
			}
			Configuration conf = this.getConf();
			Job job = Job.getInstance(super.getConf(), JOB_NAME);

			//hbase connection configuration
			Configuration hbaseConf = HBaseConfiguration.create();
			URI modelURI =  URI.createURI(inputLocation);
			hbaseConf.set("hbase.zookeeper.quorum", modelURI.host());
			hbaseConf.set("hbase.zookeeper.property.clientPort", modelURI.port() != null ? modelURI.port() : "2181");

			// A filter to skip the root value for distribution
			FilterList filterList = new FilterList(new KeyOnlyFilter(),
					new RowFilter(CompareOp.NOT_EQUAL, new RegexStringComparator("ROOT")));

			Scan scan = new Scan();
			// 500 is the recommended for MR,
			// TODO check if it is a good fit for us
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			scan.setFilter(filterList);
			//scan.setStartRow(Bytes.toBytes("ROOT"));

			// Configure classes
			job.setJarByClass(ATLMRHBaseMaster.class);
			job.setNumReduceTasks(recommendedMappers);

			String cloneURI = formatURI(modelURI);
			TableName tableName = TableName.valueOf(cloneURI);

			// mapper job initialization

			TableMapReduceUtil.initTableMapperJob(
					tableName.getNameAsString(),
					scan,
					TableATLMRMapper.class,
					LongWritable.class,
					Text.class,
					job,
					true,
					ATLMRTableInputFormat2.class
					);

			//			TableMapReduceUtil.initTableMapperJob(
			//					tableName.getNameAsString(), // tableName
			//					scan, // scanner
			//					TableATLMRMapper.class, // main class
			//					LongWritable.class, // outputKeyClass
			//					Text.class,// output value class
			//					job// the Job
			//					);

			// adding hbase resources to mapreduce
			HBaseConfiguration.addHbaseResources(job.getConfiguration());

			//			job.setMapperClass(ATLMRMapper.class);
			job.setReducerClass(ATLMRHBaseReducer.class);
			job.setOutputFormatClass(NullOutputFormat.class);

			String timestamp = new SimpleDateFormat("yyyyMMddhhmm").format(new Date());
			String outDirName = "atlmr-out-" + timestamp + "-" + UUID.randomUUID();
			FileOutputFormat.setOutputPath(job, new Path(job.getWorkingDirectory().suffix(Path.SEPARATOR + outDirName).toUri()));

			// Configure ATL related inputs/outputs
			job.getConfiguration().set(TRANSFORMATION, transformationLocation);
			job.getConfiguration().set(SOURCE_PACKAGE, sourcemmLocation);
			job.getConfiguration().set(TARGET_PACKAGE, targetmmLocation);
			job.getConfiguration().set(INPUT_MODEL, inputLocation);
			job.getConfiguration().set(OUTPUT_MODEL, outputLocation);
			job.getConfiguration().set(RECOMMENDED_MAPPERS, Integer.toString(recommendedMappers));
			job.getConfiguration().set(COUNTERS, Boolean.toString(counters));

			// CLeaning the resources if they already exist
			// target URI
			NeoEMFUtil.ResourceUtil.INSTANCE.deleteResourceIfExists(URI.createURI(outputLocation));
			// Trace URI
			NeoEMFUtil.ResourceUtil.INSTANCE.deleteResourceIfExists(URI.createURI(inputLocation+"/"+ATLMapReduceTask.TRACES_NSURI));
			// Trace Map URI
			NeoEMFUtil.ResourceUtil.INSTANCE.deleteResourceIfExists(URI.createURI(inputLocation+"/"+ATLMapReduceTask.TRACES_NSURI_MAP));

			//Starting the job
			Logger.getGlobal().log(Level.INFO, "Starting Job execution");
			long begin = System.currentTimeMillis();
			int returnValue = job.waitForCompletion(true) ? STATUS_OK : STATUS_ERROR;
			long end = System.currentTimeMillis();
			Logger.getGlobal().log(Level.INFO, MessageFormat.format("Job execution ended in {0}s with status code {1}", (end - begin) / 1000, returnValue));

			return returnValue;

		} catch (ParseException e) {
			System.err.println(e.getLocalizedMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.setOptionComparator(new OptionComarator<>());
			try {
				formatter.setWidth(Math.max(Terminal.getTerminal().getTerminalWidth(), 80));
			} catch (Throwable t) {
				// Nothing to do...
			};
			formatter.printHelp("yarn jar <this-file.jar>", options, true);
			return STATUS_ERROR;
		}
	}

	/**
	 * Configures the program options
	 *
	 * @param options
	 */
	private static void configureOptions(Options options) {

		Option transformationOpt = OptionBuilder.create(TRANSFORMATION);
		transformationOpt.setLongOpt(TRANSFORMATION_LONG);
		transformationOpt.setArgName("transformation.emftvm");
		transformationOpt.setDescription("URI of the ATL transformation file.");
		transformationOpt.setArgs(1);
		transformationOpt.setRequired(true);

		Option sourcemmOpt = OptionBuilder.create(SOURCE_PACKAGE);
		sourcemmOpt.setLongOpt(SOURCE_PACKAGE_LONG);
		sourcemmOpt.setArgName("packageName.impl.SourcePackageImpl");
		sourcemmOpt.setDescription("the name of the source packageImple");
		sourcemmOpt.setArgs(1);
		sourcemmOpt.setRequired(true);

		Option targetmmOpt = OptionBuilder.create(TARGET_PACKAGE);
		targetmmOpt.setLongOpt(TARGET_PACKAGE_LONG);
		targetmmOpt.setArgName("packageName.impl.TargetPackageImpl");
		targetmmOpt.setDescription("the name of target packageImpl.");
		targetmmOpt.setArgs(1);
		targetmmOpt.setRequired(true);

		Option inputOpt = OptionBuilder.create(INPUT_MODEL);
		inputOpt.setLongOpt(INPUT_MODEL_LONG);
		inputOpt.setArgName("neoemfhbase://host:port/inputModelName");
		inputOpt.setDescription("URI of the input model");
		inputOpt.setArgs(1);
		inputOpt.setRequired(true);

		Option outputOpt = OptionBuilder.create(OUTPUT_MODEL);
		outputOpt.setLongOpt(OUTPUT_MODEL_LONG);
		outputOpt.setArgName("neoemfhbase://host:port/outputModelName");
		outputOpt.setDescription("URI of the output file");
		outputOpt.setArgs(1);
		outputOpt.setRequired(true);

		Option recommendedMappersOption = OptionBuilder.create(RECOMMENDED_MAPPERS);
		recommendedMappersOption.setLongOpt(RECOMMENDED_MAPPERS_LONG);
		recommendedMappersOption.setArgName("mappers_hint");
		recommendedMappersOption.setDescription("The recommended number of mappers (not strict, used only as a hint). Optional, defaults to 1. Excludes the use of '-n'.");
		recommendedMappersOption.setType(Number.class);
		recommendedMappersOption.setArgs(1);

		Option recordsPerMapperOption = OptionBuilder.create(RECORDS_PER_MAPPER);
		recordsPerMapperOption.setLongOpt(RECORDS_PER_MAPPER_LONG);
		recordsPerMapperOption.setArgName("recors_per_mapper");
		recordsPerMapperOption.setDescription("Number of records to be processed by mapper. Optional, defaults to all records. Excludes the use of '-m'.");
		recordsPerMapperOption.setType(Number.class);
		recordsPerMapperOption.setArgs(1);

		OptionGroup mappersGroup = new OptionGroup();
		mappersGroup.addOption(recommendedMappersOption);
		mappersGroup.addOption(recordsPerMapperOption);

		Option quietOption = OptionBuilder.create(QUIET);
		quietOption.setLongOpt(QUIET_LONG);
		quietOption.setDescription("Do not print any information about the transformation execution on the standard output. Optional, disabled by default.");
		quietOption.setArgs(0);

		Option verboseOption = OptionBuilder.create(VERBOSE);
		verboseOption.setLongOpt(VERBOSE_LONG);
		verboseOption.setDescription("Verbose mode. Optional, disabled by default.");
		verboseOption.setArgs(0);

		Option countersOption = OptionBuilder.create(COUNTERS);
		countersOption.setLongOpt(COUNTERS_LONG);
		countersOption.setDescription("Resource statistics mode. Optional, disabled by default.");
		countersOption.setArgs(0);

		OptionGroup loggingGroup = new OptionGroup();
		loggingGroup.addOption(quietOption);
		loggingGroup.addOption(verboseOption);

		options.addOption(transformationOpt);
		options.addOption(sourcemmOpt);
		options.addOption(targetmmOpt);
		options.addOption(inputOpt);
		options.addOption(outputOpt);
		options.addOptionGroup(loggingGroup);
		options.addOptionGroup(mappersGroup);
	}

	private String formatURI(URI modelURI) {
		StringBuilder strBld = new StringBuilder();
		for (int i=0; i < modelURI.segmentCount(); i++) {
			strBld.append(modelURI.segment(i).replaceAll("-","_"));
			if (i != modelURI.segmentCount() -1 ) {
				strBld.append("_");
			}
		}

		return strBld.toString();
	}




}
