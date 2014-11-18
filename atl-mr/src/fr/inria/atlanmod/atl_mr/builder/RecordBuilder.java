package fr.inria.atlanmod.atl_mr.builder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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

	public static String SOURCE_METAMODEL = "sourcemm";
	public static String INPUT_MODEL = "input";
	public static String OUTPUT_FILE = "output";


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
			String outputLocation = commandLine.getOptionValue(OUTPUT_FILE);

			// Initialize ExtensionToFactoryMap
			Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl());
			Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());
			
			
			// Build records file
			Builder recordBuilder = new Builder(URI.createURI(inputLocation), Arrays.asList(URI.createURI(sourcemmLocation)));
			recordBuilder.save(new File(outputLocation));
			
		} catch (ParseException e) {
			System.err.println(e.getLocalizedMessage());
			HelpFormatter formatter = new HelpFormatter();
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
		sourcemmOpt.setArgName("source.ecore");
		sourcemmOpt.setDescription("Source metamodel");
		sourcemmOpt.setArgs(1);
		sourcemmOpt.setRequired(true);

		Option inputOpt = OptionBuilder.create(INPUT_MODEL);
		inputOpt.setArgName("input.xmi");
		inputOpt.setDescription("Input file");
		inputOpt.setArgs(1);
		inputOpt.setRequired(true);

		Option outputOpt = OptionBuilder.create(OUTPUT_FILE);
		outputOpt.setArgName("output.rec");
		outputOpt.setDescription("Output records file");
		outputOpt.setArgs(1);
		inputOpt.setRequired(true);

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
