package fr.inria.atlanmod.atl_mr;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ATLMRMaster extends Configured implements Tool {

	protected static final String JOB_NAME = "ATL in MapReduce";

	protected static final int STATUS_OK = 0;
	protected static final int STATUS_ERROR = 1;

	static final String TRANSFORMATION 						= "f";
	static final String SOURCE_METAMODEL 					= "s";
	static final String TARGET_METAMODEL 					= "t";
	static final String INPUT_MODEL 						= "i";
	static final String OUTPUT_MODEL 						= "o";

	private static final String RECORDS_FILE				= "r";
	private static final String RECOMMENDED_MAPPERS 		= "m";
	private static final String RECORDS_PER_MAPPER	 		= "n";
	private static final String QUIET 						= "q";
	private static final String VERBOSE 					= "v";

	private static final String TRANSFORMATION_LONG 		= "file";
	private static final String SOURCE_METAMODEL_LONG 		= "source-metamodel";
	private static final String TARGET_METAMODEL_LONG 		= "target-metamodel";
	private static final String INPUT_MODEL_LONG 			= "input";
	private static final String OUTPUT_MODEL_LONG 			= "output";
	private static final String RECORDS_FILE_LONG 			= "records";
	private static final String RECOMMENDED_MAPPERS_LONG	= "recommended-mappers";
	private static final String RECORDS_PER_MAPPER_LONG	 	= "records-per-mapper";
	private static final String QUIET_LONG 					= "quiet";
	private static final String VERBOSE_LONG 				= "verbose";

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
		int res = ToolRunner.run(conf, new ATLMRMaster(), args);
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
			String sourcemmLocation = commandLine.getOptionValue(SOURCE_METAMODEL);
			String targetmmLocation = commandLine.getOptionValue(TARGET_METAMODEL);
			String recordsLocation = commandLine.getOptionValue(RECORDS_FILE);
			String inputLocation = commandLine.getOptionValue(INPUT_MODEL);
			String outputLocation = commandLine.getOptionValue(OUTPUT_MODEL, new Path(inputLocation).suffix(".out.xmi")
					.toString());

			int recommendedMappers = 1;
			if (commandLine.hasOption(RECOMMENDED_MAPPERS)) {
				recommendedMappers = ((Number) commandLine.getParsedOptionValue(RECOMMENDED_MAPPERS)).intValue();
			}

			Configuration conf = this.getConf();
			Job job = Job.getInstance(conf, JOB_NAME);

			// Configure classes
			job.setJarByClass(ATLMRMaster.class);
			job.setMapperClass(ATLMRMapper.class);
			job.setReducerClass(ATLMRReducer.class);
			job.setInputFormatClass(NLineInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(BytesWritable.class);
			job.setNumReduceTasks(1);

			// Configure MapReduce input/outputs
			Path recordsPath = new Path(recordsLocation);
			FileInputFormat.setInputPaths(job, recordsPath);
			String timestamp = new SimpleDateFormat("yyyyMMddhhmm").format(new Date());
			String outDirName = "atlmr-out-" + timestamp + "-" + UUID.randomUUID();
			FileOutputFormat.setOutputPath(job, new Path(job.getWorkingDirectory().suffix(Path.SEPARATOR + outDirName).toUri()));

			// Configure records per map
			FileSystem fileSystem = FileSystem.get(recordsPath.toUri(), conf);
			InputStream inputStream = fileSystem.open(recordsPath);
			long linesPerMap = (long) Math.ceil((double) countLines(inputStream) / (double) recommendedMappers);
			job.getConfiguration().setLong(NLineInputFormat.LINES_PER_MAP, linesPerMap);


			// Configure ATL related inputs/outputs
			job.getConfiguration().set(TRANSFORMATION, transformationLocation);
			job.getConfiguration().set(SOURCE_METAMODEL, sourcemmLocation);
			job.getConfiguration().set(TARGET_METAMODEL, targetmmLocation);
			job.getConfiguration().set(INPUT_MODEL, inputLocation);
			job.getConfiguration().set(OUTPUT_MODEL, new Path(FileOutputFormat.getOutputPath(job).suffix(Path.SEPARATOR + outputLocation).toString()).toString());

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

		Option sourcemmOpt = OptionBuilder.create(SOURCE_METAMODEL);
		sourcemmOpt.setLongOpt(SOURCE_METAMODEL_LONG);
		sourcemmOpt.setArgName("source.ecore");
		sourcemmOpt.setDescription("URI of the source metamodel file.");
		sourcemmOpt.setArgs(1);
		sourcemmOpt.setRequired(true);

		Option targetmmOpt = OptionBuilder.create(TARGET_METAMODEL);
		targetmmOpt.setLongOpt(TARGET_METAMODEL_LONG);
		targetmmOpt.setArgName("target.ecore");
		targetmmOpt.setDescription("URI of the target metamodel file.");
		targetmmOpt.setArgs(1);
		targetmmOpt.setRequired(true);

		Option inputOpt = OptionBuilder.create(INPUT_MODEL);
		inputOpt.setLongOpt(INPUT_MODEL_LONG);
		inputOpt.setArgName("input.xmi");
		inputOpt.setDescription("URI of the input file.");
		inputOpt.setArgs(1);
		inputOpt.setRequired(true);

		Option outputOpt = OptionBuilder.create(OUTPUT_MODEL);
		outputOpt.setLongOpt(OUTPUT_MODEL_LONG);
		outputOpt.setArgName("output.xmi");
		outputOpt.setDescription("URI of the output file. Optional.");
		outputOpt.setArgs(1);

		Option recordsOpt = OptionBuilder.create(RECORDS_FILE);
		recordsOpt.setLongOpt(RECORDS_FILE_LONG);
		recordsOpt.setArgName("records.rec");
		recordsOpt.setDescription("URI of the records file.");
		recordsOpt.setArgs(1);
		recordsOpt.setRequired(true);

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

		OptionGroup loggingGroup = new OptionGroup();
		loggingGroup.addOption(quietOption);
		loggingGroup.addOption(verboseOption);

		options.addOption(transformationOpt);
		options.addOption(sourcemmOpt);
		options.addOption(targetmmOpt);
		options.addOption(recordsOpt);
		options.addOption(inputOpt);
		options.addOption(outputOpt);
		options.addOptionGroup(loggingGroup);
		options.addOptionGroup(mappersGroup);
	}

	private static long countLines(InputStream inputStream) throws IOException {
		LineNumberReader lineNumberReader = null;
		int lines = 1;
		try {
			lineNumberReader = new LineNumberReader(new InputStreamReader(inputStream));
			lineNumberReader.skip(Long.MAX_VALUE);
			lines = Math.max(lineNumberReader.getLineNumber(), 1);
		} finally {
			if (lineNumberReader != null) {
				lineNumberReader.close();
			}
		}
		return lines;
	}
}
