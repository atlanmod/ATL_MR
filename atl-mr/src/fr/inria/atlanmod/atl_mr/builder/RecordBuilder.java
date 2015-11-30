package fr.inria.atlanmod.atl_mr.builder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import jline.Terminal;

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
import org.eclipse.emf.ecore.xmi.impl.EcoreResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;

import fr.inria.atlanmod.atl_mr.utils.ATLMRUtils;

public class RecordBuilder  {

	static final String SOURCE_METAMODEL 					= "s";
	static final String INPUT_MODEL 						= "i";
	static final String OUTPUT_FILE 						= "o";

	private static final String SOURCE_METAMODEL_LONG 		= "source-metamodel";
	private static final String INPUT_MODEL_LONG 			= "input";
	private static final String OUTPUT_FILE_LONG 			= "output";

	private static class OptionComparator<T extends Option> implements Comparator<T> {
		private static final String OPTS_ORDER = "sio";

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

			String sourcemmLocation = commandLine.getOptionValue(SOURCE_METAMODEL);
			String inputLocation = commandLine.getOptionValue(INPUT_MODEL);
			String outputLocation = commandLine.getOptionValue(OUTPUT_FILE) != null ?
					commandLine.getOptionValue(OUTPUT_FILE) : inputLocation.concat(".rec");

					// Initialize ExtensionToFactoryMap
					Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl());
					Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());


					// Build records file
					Builder recordBuilder = new Builder(URI.createURI(inputLocation), Arrays.asList(URI.createURI(sourcemmLocation)));
					recordBuilder.save(new File(outputLocation));

		} catch (ParseException e) {
			System.err.println(e.getLocalizedMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.setOptionComparator(new OptionComparator<>());
			try {
				formatter.setWidth(Math.max(Terminal.getTerminal().getTerminalWidth(), 80));
			} catch (Throwable t) {
				// Nothing to do...
			};
			formatter.printHelp("java -jar <this-file.jar>", options, true);
		}
	}

	/**
	 * Configures the program options
	 *
	 * @param options
	 */
	private static void configureOptions(Options options) {

		Option sourcemmOpt = OptionBuilder.create(SOURCE_METAMODEL);
		sourcemmOpt.setLongOpt(SOURCE_METAMODEL_LONG);
		sourcemmOpt.setArgName("source.ecore");
		sourcemmOpt.setDescription("URI of the source metamodel file.");
		sourcemmOpt.setArgs(1);
		sourcemmOpt.setRequired(true);

		Option inputOpt = OptionBuilder.create(INPUT_MODEL);
		inputOpt.setLongOpt(INPUT_MODEL_LONG);
		inputOpt.setArgName("input.xmi");
		inputOpt.setDescription("URI of the input file.");
		inputOpt.setArgs(1);
		inputOpt.setRequired(true);

		Option outputOpt = OptionBuilder.create(OUTPUT_FILE);
		outputOpt.setLongOpt(OUTPUT_FILE_LONG);
		outputOpt.setArgName("records.rec");
		outputOpt.setDescription("Path of the output records file.");
		outputOpt.setArgs(1);

		options.addOption(sourcemmOpt);
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
	public static class Builder {

		private ResourceSet resourceSet;

		private List<URI> metamodelURIs;

		private URI inputURI;

		public Builder(URI inputURI, List<URI> metamodelURIs) {
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
