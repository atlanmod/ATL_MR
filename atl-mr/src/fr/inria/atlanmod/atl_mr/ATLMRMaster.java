package fr.inria.atlanmod.atl_mr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
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
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.impl.EcoreResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;

import fr.inria.atlanmod.atl_mr.utils.ATLMRUtils;

public class ATLMRMaster extends Configured implements Tool {

	protected static final String JOB_NAME = "ATL in MapReduce";

	protected static final int STATUS_OK = 0;
	protected static final int STATUS_ERROR = 1;


	public static String TRANSFORMATION = "transformation";
	public static String SOURCE_METAMODEL = "sourcemm";
	public static String TARGET_METAMODEL = "targetmm";
	public static String INPUT_MODEL = "input";
	public static String OUTPUT_MODEL = "output";

	{
		// Initialize ExtensionToFactoryMap
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl());
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("emftvm", new EMFTVMResourceFactoryImpl());
	}

	/**
	 * Main program, delegates to ToolRunner
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ATLMRMaster driver = new ATLMRMaster();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
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
			String inputLocation = commandLine.getOptionValue(INPUT_MODEL);
			String outputLocation = commandLine.getOptionValue(OUTPUT_MODEL, Paths.get(inputLocation).getParent().resolve("output.xmi").toString());

			Job job = Job.getInstance(getConf(), JOB_NAME);

			// TODO: check number of lines per MAP
			getConf().setInt(NLineInputFormat.LINES_PER_MAP, 5);

			{ 
				// Configure classes
				job.setJarByClass(ATLMRMaster.class);
				job.setMapperClass(ATLMRMapper.class);
				job.setReducerClass(ATLMRReducer.class);
				job.setInputFormatClass(NLineInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(BytesWritable.class);
			}
			

			{
				// Build records file 
				RecordBuilder recordBuilder = new RecordBuilder(URI.createURI(inputLocation), Arrays.asList(URI.createURI(sourcemmLocation)));
				File recordsFile = File.createTempFile("atlmr-", ".rcd", new File(job.getWorkingDirectory().suffix("/working").toUri()));
				recordsFile.deleteOnExit();
				recordBuilder.save(recordsFile);
				
				// Configure MapReduce input/outputs
				FileInputFormat.setInputPaths(job, new Path(recordsFile.toURI()));
				FileOutputFormat.setOutputPath(job, new Path(job.getWorkingDirectory().suffix("/working").suffix("/" + UUID.randomUUID()).toUri()));
			}

			{
				// Configure ATL related inputs/outputs
				job.getConfiguration().set(TRANSFORMATION, transformationLocation);
				job.getConfiguration().set(SOURCE_METAMODEL, sourcemmLocation);
				job.getConfiguration().set(TARGET_METAMODEL, targetmmLocation);
				job.getConfiguration().set(INPUT_MODEL, inputLocation);
				job.getConfiguration().set(OUTPUT_MODEL, outputLocation);
			}
			
			return job.waitForCompletion(true) ? STATUS_OK : STATUS_ERROR;

		} catch (ParseException e) {
			System.err.println(e.getLocalizedMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar <this-file.jar>", options, true);
			return STATUS_ERROR;
		}

	}

	
	/**
	 * Configures the program options
	 * 
	 * @param options
	 */
	private void configureOptions(Options options) {
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

		Option inputOpt = OptionBuilder.create(INPUT_MODEL);
		inputOpt.setArgName("input.xmi");
		inputOpt.setDescription("Input file URI");
		inputOpt.setArgs(1);
		inputOpt.setRequired(true);

		Option outputOpt = OptionBuilder.create(OUTPUT_MODEL);
		outputOpt.setArgName("output.xmi");
		outputOpt.setDescription("Output file URI");
		outputOpt.setArgs(1);

		options.addOption(transformationOpt);
		options.addOption(sourcemmOpt);
		options.addOption(targetmmOpt);
		options.addOption(inputOpt);
		options.addOption(outputOpt);
	}

	/**
	 * Implements the logic to build MapReduce records from a {@link Resource}
	 * given its {@link URI}
	 * 
	 * @author agomez
	 *
	 */
	public static class RecordBuilder {

		private ResourceSet resourceSet;

		private List<URI> metamodelURIs;

		private URI inputURI;

		public RecordBuilder(URI inputURI, List<URI> metamodelURIs) {
			this.inputURI = inputURI;
			this.metamodelURIs = metamodelURIs;
		}

		protected ResourceSet getResourceSet() {
			if (resourceSet == null) {
				resourceSet = new ResourceSetImpl();
				for (URI uri : metamodelURIs) {
					Resource resource = resourceSet.getResource(uri, true);
					ATLMRUtils.registerPackages(resourceSet, resource);
				}
			}
			return resourceSet;
		}

		/**
		 * Saves the records of this {@link RecordBuilder} in the given
		 * {@link File}
		 * 
		 * @param file
		 * @throws FileNotFoundException
		 * @throws IOException
		 */
		public void save(File file) throws FileNotFoundException, IOException {
			Resource inputResource = getResourceSet().getResource(inputURI, true);
			buildRecords(inputResource, new FileOutputStream(file));
			inputResource.unload();
		}

		/**
		 * Writes on a given {@link OutputStream} the records for a
		 * {@link Resource}
		 * 
		 * @param inputResource
		 * @param outputStream
		 * @throws IOException
		 */
		protected static void buildRecords(Resource inputResource, OutputStream outputStream) throws IOException {

			if (!inputResource.isLoaded()) {
				throw new IllegalArgumentException("Input resource is not loaded");
			}

			BufferedWriter bufWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

			for (Iterator<EObject> it = inputResource.getAllContents(); it.hasNext();) {
				EObject currentObj = it.next();
				bufWriter.append("<");
				bufWriter.append(currentObj.eResource().getURIFragment(currentObj));
				bufWriter.append(",");
				bufWriter.append(currentObj.eClass().getName());
				bufWriter.append(">\n");
			}

			bufWriter.close();
		}

	}
}
