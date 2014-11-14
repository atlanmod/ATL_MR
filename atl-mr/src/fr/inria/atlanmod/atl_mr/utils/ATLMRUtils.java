package fr.inria.atlanmod.atl_mr.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.m2m.atl.emftvm.Rule;
import org.eclipse.m2m.atl.emftvm.trace.TraceFactory;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TraceLinkSet;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;

public class ATLMRUtils {

	/**
	 * Returns the Working directory
	 * @return
	 */
	public static File getWorkingDirectoryFile() {
		return Paths.get("").toAbsolutePath().toFile();
	}
	/**
	 * This method is used to extract rules from a 
	 * @param resource
	 * @return
	 * @throws IOException
	 */
	public static HashMap<String, String> extractRules(Resource resource) throws IOException {

		HashMap<String, String> map = new HashMap<String, String>();
		if (!resource.isLoaded()) {
			resource.load(null);
		}

		Iterator<EObject> iterator = resource.getAllContents();
		EObject objectIterator = null;
		while (iterator.hasNext()) {
			objectIterator = iterator.next();
			if (objectIterator instanceof Rule) {
				Rule rule = ((Rule) objectIterator);
				map.put(rule.getInputElements().get(0).getEType().getName(), rule.getName());

			}
		}
		return map;

	}
	
	/**
	 * Registers the packages
	 * @param resourceSet
	 * @param resource
	 */
	public static void registerPackages(ResourceSet resourceSet, Resource resource) {
		EObject eObject = resource.getContents().get(0);
		if (eObject instanceof EPackage) {
			EPackage p = (EPackage) eObject;
			resourceSet.getPackageRegistry().put(p.getNsURI(), p);
		}

	}
	
	/**
	 * Resolves the output path when the user does not provide it 
	 * @param string
	 * @return
	 */
	public static String resolveOutputPath(String string) {
		StringBuilder builder = new StringBuilder(string.substring(0, string.lastIndexOf(".")));
		builder.append("_out");
		builder.append(string.substring(string.lastIndexOf('.')));
		return builder.toString();
	}
	
	/**
	 * Copies a rule in order to be serialized and passed to the reduced
	 * @param currentRule
	 * @param currentLink
	 * @return
	 */
	public static TracedRule copyRule(TracedRule currentRule, TraceLink currentLink) {
		TraceLinkSet set = TraceFactory.eINSTANCE.createTraceLinkSet();
		TracedRule tracedRule = TraceFactory.eINSTANCE.createTracedRule();
		TraceLink newLink = EcoreUtil.copy(currentLink);
		tracedRule.setLinkSet(set);
		tracedRule.setRule(currentRule.getRule());
		newLink.setRule(tracedRule);
		return tracedRule;
	}

}
