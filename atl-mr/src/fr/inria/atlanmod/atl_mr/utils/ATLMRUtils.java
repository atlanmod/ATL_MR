package fr.inria.atlanmod.atl_mr.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.OutputRuleElement;
import org.eclipse.m2m.atl.emftvm.Rule;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceImpl;
import org.eclipse.m2m.atl.emftvm.trace.TargetElement;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TraceLinkSet;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;

public class ATLMRUtils {
	
	public static Resource importToXML (Resource binaryResource) {
		if (!(binaryResource instanceof EMFTVMResourceImpl)) return null;
		
		ResourceSetImpl rset = new ResourceSetImpl();
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());	
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("emftvm", new EMFTVMResourceFactoryImpl());
		URI xmiURI = addXMIExtension(binaryResource.getURI());
		Resource xmiResource = rset.createResource(xmiURI);
		xmiResource.getContents().addAll(EcoreUtil.copyAll(binaryResource.getContents()));
		try {
			xmiResource.save(null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return xmiResource;
	}

	private static URI addXMIExtension( URI uri) {
//		String scheme = uri.scheme()!= null ? uri.scheme() : "";
//		String authority = uri.authority() != null ? uri.authority() : "";
//		String device = uri.device() ;//!= null ? uri.device() : "";
//		String query = uri.query() != null ? uri.query() : "";
//		String fragment =  uri.fragment() != null ? uri.fragment() : "";
		return URI.createFileURI(uri.toFileString().concat(".xmi"));//URI(scheme,authority ,device , query, fragment.concat(".xmi"));
	}
	
	
	public static File writeRecordsToFile(Resource transformationResource, Resource inputResource) throws IOException {
		
		File file = File.createTempFile("records", ".rcd.tmp");
//		HashMap<String, String> rules = extractRules(transformationResource);
		
		if (! inputResource.isLoaded()) {
			inputResource.load(null);
		}
		
		Iterator<EObject> treeIterator = inputResource.getAllContents();
		EObject currentObj = null;
		
		FileWriter writer = new FileWriter(file);
		BufferedWriter bufWriter = new BufferedWriter(writer);
		
		while(treeIterator.hasNext()) {
			
			currentObj =  treeIterator.next();
			StringBuilder record = new StringBuilder("<");
			String fragment = currentObj.eResource().getURIFragment(currentObj);
//			EcoreUtil.getIdentification(currentObj);
			//EcoreUtil.setID(currentObj, UUID);
			record.append(fragment);
			record.append(",");
			record.append(currentObj.eClass().getName());
			record.append(">\n");
		
			bufWriter.write(record.toString());
			
		}
			bufWriter.close();
			
			inputResource.save(null);
		return file;
	}
	
	public static HashMap<String, String> extractRules (Resource resource) throws IOException {
		
		HashMap<String, String> map = new HashMap<String, String>();
		if(! resource.isLoaded()) {
			resource.load(null);
		}

		Iterator<EObject> iterator = resource.getAllContents();
		EObject objectIterator= null;
		while (iterator.hasNext()) {
			objectIterator = iterator.next();
			if (objectIterator instanceof Rule) {
				Rule rule = ((Rule)objectIterator);
				map.put(rule.getInputElements().get(0).getEType().getName(), rule.getName() );
				
			}
		}	
		return map;
		
	}	
	
//	private static HashMap<String, String> extractRules (URI uri) throws IOException {
//		
//		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("emftvm", new EMFTVMResourceFactoryImpl());
//		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());
//		
//		ResourceSetImpl rset = new ResourceSetImpl();
//		Resource resource = rset.createResource(uri);
//		Assert.assertNotNull(resource);
//		return extractRules(resource);
//		
//	}
	
	public static void registerPackages(ResourceSet rs, Resource resource) {
		
		EObject eObject = resource.getContents().get(0);
		if (eObject instanceof EPackage) {
		    EPackage p = (EPackage)eObject;
		    rs.getPackageRegistry().put(p.getNsURI(), p);
		}	
	  	
	}

	public static String resolveOutputPath(String string) {
		StringBuilder builder = new StringBuilder(string.substring(0, string.lastIndexOf(".")));
		builder.append("_out");
		builder.append(string.substring(string.lastIndexOf('.')));
		return builder.toString();
	}

	
	public static void mergeTraces(ExecEnv executionEnv, TracedRule tracedRule, ResourceSet rs) {
		
	    //createElement 
		TraceLinkSet traces = executionEnv.getTraces();
		TraceLink traceLink = tracedRule.getLinks().get(0);
		EObject targetElement = traceLink.getSourceElements().get(0).getObject();
		EcoreUtil.resolve(targetElement, rs);
		traceLink.getSourceElements().get(0).setRuntimeObject(targetElement);
		Rule rule = executionEnv.getRulesMap().get(tracedRule.getRule());
		int indexer = 0;
		for (OutputRuleElement ore : rule.getOutputElements()) {
			
			EClass type=null;
			try {
				type = (EClass)executionEnv.findType(ore.getTypeModel(), ore.getType());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				//throw new VMException();
			}
			TargetElement te = traceLink.getTargetElements().get(indexer);
			// supposing that the outputRuleElement preserves its order
			EList<Model> models = ore.getEModels();
			assert models.size() == 1;
			te.setObject(models.get(0).newElement(type));
			assert te.getObject() != null;
			assert te.getObject().eResource() != null;
			assert te.getObject().eResource() == models.get(0).getResource();
			indexer++;
		}
		
		boolean notApplied = true;
		for (Iterator<TracedRule> iter = traces.getRules().iterator(); iter.hasNext() && notApplied;) {
			TracedRule tRule = iter.next();
			if (tRule.getRule().equals(tracedRule.getRule())) {
				tRule.getLinks().add(tracedRule.getLinks().get(0));
				notApplied = false;
			}
		}
		if (notApplied) {
			traces.getRules().add(tracedRule);
		}
	}
	
}
