/*******************************************************************************
 * Copyright (c) 2014 Abel G�mez.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Abel G�mez - initial API and implementation
 *     Amine Benelallam
 ******************************************************************************/
package fr.inria.atlanmod.atl_mr.utils;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.XMIResource;
import org.eclipse.emf.ecore.xmi.impl.EcoreResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;

import fr.inria.atlanmod.neoemf.core.NeoEMFResourceFactory;
import fr.inria.atlanmod.neoemf.core.impl.NeoEMFHbaseResourceImpl;
import fr.inria.atlanmod.neoemf.util.NeoEMFURI;

public class NeoEMFHBaseMigrator {

	private static final Logger LOG = Logger.getLogger(NeoEMFHBaseMigrator.class.getName());

	private static final String IN = "i";
	private static final String OUT = "o";
	private static final String E_PACKAGE = "p";

	private static final String IN_LONG = "input";
	private static final String OUT_LONG = "output";
	private static final String E_PACKAGE_LONG = "package";


	public static void main(String[] args) {
		Options options = new Options();

		Option inputOpt = OptionBuilder.create(IN);
		inputOpt.setLongOpt(IN_LONG);
		inputOpt.setArgName("INPUT");
		inputOpt.setDescription("Input file, both of xmi and zxmi extensions are supported");
		inputOpt.setArgs(1);
		inputOpt.setRequired(true);


		Option outputOpt = OptionBuilder.create(OUT);
		outputOpt.setLongOpt(OUT_LONG);
		outputOpt.setArgName("OUTPUT");
		outputOpt.setDescription("Output HBase resource URI. <e.g neoemfhbase://host:port/output_location>");
		outputOpt.setArgs(1);
		outputOpt.setRequired(true);

		Option inClassOpt = OptionBuilder.create(E_PACKAGE);
		inClassOpt.setLongOpt(E_PACKAGE_LONG);
		inClassOpt.setArgName("EPACKAGE_IMPL");
		inClassOpt.setDescription("the FQN of the package implementation class. <e.g. fr.example.impl.ExamplePackageImpl>");
		inClassOpt.setArgs(1);
		inClassOpt.setRequired(true);

		options.addOption(inputOpt);
		options.addOption(outputOpt);
		options.addOption(inClassOpt);

		CommandLineParser parser = new PosixParser();

		try {
			CommandLine commandLine = parser.parse(options, args);

			URI sourceUri = URI.createFileURI(commandLine.getOptionValue(IN));
			URI targetUri = URI.createURI(commandLine.getOptionValue(OUT));
			//URI metamodelUri = URI.createFileURI(commandLine.getOptionValue(E_PACKAGE));

			// registering the metamodel package
			NeoEMFHBaseMigrator.class.getClassLoader().loadClass(commandLine.getOptionValue(E_PACKAGE)).getMethod("init").invoke(null);


			ResourceSet resourceSet = new ResourceSetImpl();
			resourceSet.getResourceFactoryRegistry().getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl());
			resourceSet.getResourceFactoryRegistry().getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());
			resourceSet.getResourceFactoryRegistry().getExtensionToFactoryMap().put("zxmi", new XMIResourceFactoryImpl());
			resourceSet.getResourceFactoryRegistry().getProtocolToFactoryMap().put(NeoEMFURI.NEOEMF_HBASE_SCHEME, NeoEMFResourceFactory.eINSTANCE);

			Resource sourceResource = resourceSet.createResource(sourceUri);
			Map<String, Object> loadOpts = new HashMap<String, Object>();

			if ("zxmi".equals(sourceUri.fileExtension())) {
				loadOpts.put(XMIResource.OPTION_ZIP, Boolean.TRUE);
			}

			Runtime.getRuntime().gc();
			long initialUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			LOG.log(Level.INFO, MessageFormat.format("Used memory before loading: {0}",
					ATLMRUtils.byteCountToDisplaySize(initialUsedMemory)));
			LOG.log(Level.INFO, "Loading source resource");
			sourceResource.load(loadOpts);
			LOG.log(Level.INFO, "Source resource loaded");
			Runtime.getRuntime().gc();
			long finalUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
			LOG.log(Level.INFO, MessageFormat.format("Used memory after loading: {0}",
					ATLMRUtils.byteCountToDisplaySize(finalUsedMemory)));
			LOG.log(Level.INFO, MessageFormat.format("Memory use increase: {0}",
					ATLMRUtils.byteCountToDisplaySize(finalUsedMemory - initialUsedMemory)));


			Resource targetResource = resourceSet.createResource(targetUri);

			Map<String, Object> saveOpts = new HashMap<String, Object>();
			targetResource.save(saveOpts);

			LOG.log(Level.INFO, "Start moving elements");
			targetResource.getContents().clear();
			targetResource.getContents().addAll(sourceResource.getContents());
			LOG.log(Level.INFO, "End moving elements");
			LOG.log(Level.INFO, "Start saving");
			targetResource.save(saveOpts);
			LOG.log(Level.INFO, "Saved");

			if (targetResource instanceof NeoEMFHbaseResourceImpl) {
				NeoEMFHbaseResourceImpl.shutdownWithoutUnload((NeoEMFHbaseResourceImpl) targetResource);
			} else {
				targetResource.unload();
			}

		} catch (ParseException e) {
			ATLMRUtils.showError(e.toString());
			ATLMRUtils.showError("Current arguments: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar <this-file.jar>", options, true);
		} catch (Throwable e) {
			ATLMRUtils.showError(e.toString());
			e.printStackTrace();
		}
	}
}
