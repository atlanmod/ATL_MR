package fr.inria.atlanmod.atl_mr;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

	public static String TRANSFORMATION = "transformation";
	public static String SOURCE_METAMODEL = "sourcemm";
	public static String TARGET_METAMODEL = "targetmm";
	public static String RECORDS_FILE = "records";
	public static String INPUT_MODEL = "input";
	public static String OUTPUT_MODEL = "output";
	public static String RECORDS_PER_NODE = "rpn";

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

		CommandLineParser parser = new PosixParser();

		try {
			CommandLine commandLine = parser.parse(options, args);

			String transformationLocation = commandLine.getOptionValue(TRANSFORMATION);
			String sourcemmLocation = commandLine.getOptionValue(SOURCE_METAMODEL);
			String targetmmLocation = commandLine.getOptionValue(TARGET_METAMODEL);
			String recordsLocation = commandLine.getOptionValue(RECORDS_FILE);
			String inputLocation = commandLine.getOptionValue(INPUT_MODEL);
			String outputLocation = commandLine.getOptionValue(OUTPUT_MODEL, new Path(inputLocation).suffix(".out.xmi")
					.toString());

			Configuration conf = this.getConf();
			Job job = Job.getInstance(conf, JOB_NAME);

			if (commandLine.hasOption(RECORDS_PER_NODE)) {
				getConf().setInt(NLineInputFormat.LINES_PER_MAP, ((Number) commandLine.getParsedOptionValue(RECORDS_PER_NODE)).intValue());
			}

			// Configure classes
			job.setJarByClass(ATLMRMaster.class);
			job.setMapperClass(ATLMRMapper.class);
			job.setReducerClass(ATLMRReducer.class);
			job.setInputFormatClass(NLineInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(BytesWritable.class);

			// Configure MapReduce input/outputs
			Path recordsPath = new Path(recordsLocation);
			FileInputFormat.setInputPaths(job, recordsPath);
			String timestamp = new SimpleDateFormat("yyyyMMddhhmm").format(new Date());
			String outDirName = "atlmr-out-" + timestamp + "-" + UUID.randomUUID();
			FileOutputFormat.setOutputPath(job, new Path(job.getWorkingDirectory().suffix(Path.SEPARATOR + outDirName).toUri()));

			// Configure ATL related inputs/outputs
			job.getConfiguration().set(TRANSFORMATION, transformationLocation);
			job.getConfiguration().set(SOURCE_METAMODEL, sourcemmLocation);
			job.getConfiguration().set(TARGET_METAMODEL, targetmmLocation);
			job.getConfiguration().set(INPUT_MODEL, inputLocation);
			job.getConfiguration().set(OUTPUT_MODEL, new Path(FileOutputFormat.getOutputPath(job).suffix(Path.SEPARATOR + outputLocation).toString()).toString());

			int returnValue = job.waitForCompletion(true) ? STATUS_OK : STATUS_ERROR;

			return returnValue;

		} catch (ParseException e) {
			System.err.println(e.getLocalizedMessage());
			HelpFormatter formatter = new HelpFormatter();
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
		transformationOpt.setArgName("transformation.emftvm");
		transformationOpt.setDescription("ATL transformation");
		transformationOpt.setArgs(1);
		transformationOpt.setRequired(true);

		Option sourcemmOpt = OptionBuilder.create(SOURCE_METAMODEL);
		sourcemmOpt.setArgName("source.ecore");
		sourcemmOpt.setDescription("Source metamodel");
		sourcemmOpt.setArgs(1);
		sourcemmOpt.setRequired(true);

		Option targetmmOpt = OptionBuilder.create(TARGET_METAMODEL);
		targetmmOpt.setArgName("target.ecore");
		targetmmOpt.setDescription("Target metamodel");
		targetmmOpt.setArgs(1);
		targetmmOpt.setRequired(true);

		Option recordsOpt = OptionBuilder.create(RECORDS_FILE);
		recordsOpt.setArgName("records.rec");
		recordsOpt.setDescription("Records file");
		recordsOpt.setArgs(1);
		recordsOpt.setRequired(true);

		Option inputOpt = OptionBuilder.create(INPUT_MODEL);
		inputOpt.setArgName("input.xmi");
		inputOpt.setDescription("Input file URI");
		inputOpt.setArgs(1);
		inputOpt.setRequired(true);

		Option outputOpt = OptionBuilder.create(OUTPUT_MODEL);
		outputOpt.setArgName("output.xmi");
		outputOpt.setDescription("Output file URI");
		outputOpt.setArgs(1);

		Option recordsPerNodeOption = OptionBuilder.create(RECORDS_PER_NODE);
		recordsPerNodeOption.setArgName("records_per_node");
		recordsPerNodeOption.setDescription("Numbers of records to be processed by each node");
		recordsPerNodeOption.setType(Number.class);
		recordsPerNodeOption.setArgs(1);

		options.addOption(transformationOpt);
		options.addOption(sourcemmOpt);
		options.addOption(targetmmOpt);
		options.addOption(recordsOpt);
		options.addOption(inputOpt);
		options.addOption(outputOpt);
		options.addOption(recordsPerNodeOption);
	}
}
