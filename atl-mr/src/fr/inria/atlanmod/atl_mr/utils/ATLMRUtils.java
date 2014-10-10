package fr.inria.atlanmod.atl_mr.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.Rule;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceImpl;

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
		HashMap<String, String> rules = extractRules(transformationResource);
		
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
			String UUID = EcoreUtil.getIdentification(currentObj);
			//EcoreUtil.setID(currentObj, UUID);
			record.append(UUID);
			record.append(",");
			record.append(rules.get(currentObj.eClass().getName()));
			record.append(">\n");
		
			bufWriter.write(record.toString());
			
		}
			bufWriter.close();
			
			inputResource.save(null);
		return file;
	}
	private static HashMap<String, String> extractRules (Resource resource) throws IOException {
		
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
				map.put(rule.getInputElements().get(0).getName(), rule.getName() );
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
	
}
