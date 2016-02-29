package fr.inria.atlanmod.atl_mr.builder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;

import fr.inria.atlanmod.atl_mr.hbase.ATLMapReduceTask;
import fr.inria.atlanmod.atl_mr.utils.ATLMRUtils;
import fr.inria.atlanmod.neoemf.core.impl.NeoEMFResourceFactoryImpl;
import fr.inria.atlanmod.neoemf.util.NeoEMFURI;

public class HbaseRecordBuilder  {

	static final String INPUT_MODEL 						= "i";
	static final String OUTPUT_FILE 						= "o";
	static final String INPUT_PACKAGE 					    = "p";

	private static final String INPUT_MODEL_LONG 			= "input";
	private static final String OUTPUT_FILE_LONG 			= "output";
	private static final String INPUT_PACKAGE_LONG 		= "package";

	private static class OptionComarator<T extends Option> implements Comparator<T> {
		private static final String OPTS_ORDER = "iop";

		@Override
		public int compare(T o1, T o2) {
			return OPTS_ORDER.indexOf(o1.getOpt()) - OPTS_ORDER.indexOf(o2.getOpt());
		}
	}



	/**
	 * Main program
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		configureOptions(options);

		CommandLineParser parser = new PosixParser();

		try {
			CommandLine commandLine = parser.parse(options, args);

			String inputLocation = commandLine.getOptionValue(INPUT_MODEL);
			String outputLocation = commandLine.getOptionValue(OUTPUT_FILE) != null ? commandLine.getOptionValue(OUTPUT_FILE) : inputLocation.concat(".rec");
			String packageName = commandLine.getOptionValue(INPUT_PACKAGE);
			//			JavaPackageImpl.init();

			// Initialize ExtensionToFactoryMap
			Resource.Factory.Registry.INSTANCE.getProtocolToFactoryMap().put(NeoEMFURI.NEOEMF_HBASE_SCHEME, new NeoEMFResourceFactoryImpl());
			invokePackageInit(packageName);
			// Build records file
			Builder recordBuilder = new Builder(URI.createURI(inputLocation));
			recordBuilder.save(new File(outputLocation));

		} catch (ParseException e) {
			System.err.println(e.getLocalizedMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.setOptionComparator(new OptionComarator<>());
			formatter.printHelp("java -jar <this-file.jar>", options, true);
		}
	}

	private static EObject invokePackageInit(String MMpackageName) {
		EObject _package = null;
		try {
			_package = (EObject)ATLMapReduceTask.class.getClassLoader().loadClass(MMpackageName).getMethod("init").invoke(null);
		} catch (Exception e) {
			ATLMRUtils.showError(e.getLocalizedMessage());
			e.printStackTrace();
		}
		return _package;
	}
	/**
	 * Configures the program options
	 *
	 * @param options
	 */
	private static void configureOptions(Options options) {

		Option inputOpt = OptionBuilder.create(INPUT_MODEL);
		inputOpt.setLongOpt(INPUT_MODEL_LONG);
		inputOpt.setArgName("neoemfhbase://host:port/input_model");
		inputOpt.setDescription("URI of the input model.");
		inputOpt.setArgs(1);
		inputOpt.setRequired(true);

		Option packageOpt = OptionBuilder.create(INPUT_PACKAGE);
		packageOpt.setLongOpt(INPUT_PACKAGE_LONG);
		packageOpt.setArgName("fr.example.impl.ExamplePackageImpl");
		packageOpt.setDescription("FQN of the package");
		packageOpt.setArgs(1);
		packageOpt.setRequired(true);

		Option outputOpt = OptionBuilder.create(OUTPUT_FILE);
		outputOpt.setLongOpt(OUTPUT_FILE_LONG);
		outputOpt.setArgName("records.rec");
		outputOpt.setDescription("Path of the output records file.");
		outputOpt.setArgs(1);

		options.addOption(inputOpt);
		options.addOption(outputOpt);
		options.addOption(packageOpt);
	}


	/**
	 * Implements the logic to build MapReduce records from a {@link Resource}
	 * given its {@link URI}
	 *
	 * @author agomez
	 *
	 */
	public static class Builder {

		private ResourceSet resourceSet;

		private URI inputURI;

		public Builder(URI inputURI) {
			this.inputURI = inputURI;
		}

		protected ResourceSet getResourceSet() {
			if (resourceSet == null) {
				resourceSet = new ResourceSetImpl();
			}
			return resourceSet;
		}

		/**
		 * Saves the records of this {@link Builder} in the given
		 * {@link File}
		 *
		 * @param file
		 * @throws FileNotFoundException
		 * @throws IOException
		 */
		public void save(File file) throws FileNotFoundException, IOException {
			save(new FileOutputStream(file));
		}

		/**
		 * Saves the records of this {@link Builder} in the given
		 * {@link OutputStream}
		 *
		 * @param outputStream
		 * @throws IOException
		 */
		public void save(OutputStream outputStream) throws IOException {
			Resource inputResource = getResourceSet().getResource(inputURI, true);
			buildRecords(inputResource, outputStream);
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
				bufWriter.append(inputResource.getURIFragment(currentObj));
				bufWriter.append(",");
				bufWriter.append(currentObj.eClass().getName());
				bufWriter.append(">\n");
			}

			bufWriter.close();
		}

	}
}
