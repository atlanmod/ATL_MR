package fr.inria.atlanmod.atl_mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.eclipse.m2m.atl.emftvm.trace.TargetElement;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.BinaryResourceImpl;
import org.eclipse.emf.ecore.resource.impl.ResourceImpl;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.xmi.XMLResource;
import org.eclipse.emf.ecore.xmi.impl.EcoreResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.EmftvmFactory;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.Metamodel;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.OutputRuleElement;
import org.eclipse.m2m.atl.emftvm.Rule;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TraceLinkSet;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;
import org.eclipse.m2m.atl.emftvm.util.DefaultModuleResolver;
import org.eclipse.m2m.atl.emftvm.util.ModuleResolver;
import org.eclipse.m2m.atl.emftvm.util.TimingData;



public  class ATLMRReducer extends Reducer<Text,BytesWritable, Text, Text> {
		
	private ExecEnv executionEnv;
	
	private ResourceSetImpl rs;
	
	private ModuleResolver mr;

	private Path [] localFiles;
	
	private Model inModel;

	private String moduleName= "Families2Persons";
	
	private Model outModel= EmftvmFactory.eINSTANCE.createModel();
	
	private static Logger logger = Logger.getGlobal();
	
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,
			Context context)
			
			throws IOException, InterruptedException {		
		// Parallelize this 
				Map<String, Object> options = new HashMap<String, Object>();
				options.put(XMLResource.OPTION_BINARY, Boolean.TRUE);
				TraceLinkSet traces =  executionEnv.getTraces();	
				ByteArrayInputStream bais = null;
			    	Iterator<BytesWritable> links = values.iterator();
			    	for (BytesWritable b; links.hasNext(); ) {
			    		b= links.next();
						bais = new ByteArrayInputStream(b.getBytes());
						Resource resource = new BinaryResourceImpl();
						rs.getResources().add(resource);
						resource.load(bais, Collections.emptyMap());
						mergeTraces (traces, (TracedRule)resource.getContents().get(0));					
			    }
			outModel.getResource().save(System.out, Collections.emptyMap());
			executionEnv.applyAll();
			outModel.getResource().save(System.out, Collections.emptyMap());
			logger.log(Level.INFO, String.format("enter the reduce for key <%s>", key.toString()));		
	}
	
	@Override 
	protected void setup(Context context) {
		
		logger.log(Level.INFO, " Enter Reduce node setup for job "+context.getJobID().getId());
		localFiles = getSharedResources(context.getConfiguration());
		logger.log(Level.INFO, " Source MM uri is  "+localFiles[ATLMRMaster.SOURCE_METAMODEL_ID]);
		logger.log(Level.INFO, " Target MM uri is  "+localFiles[ATLMRMaster.TARGET_METAMODEL_ID]);
		logger.log(Level.INFO, " Input model uri is  "+localFiles[ATLMRMaster.INPUT_MODEL_ID]);

		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl());
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());	
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("emftvm", new EMFTVMResourceFactoryImpl());
		
		executionEnv = EmftvmFactory.eINSTANCE.createExecEnv();
		rs = new ResourceSetImpl();
		
		//Resource emftvmResource = rs.getResource(URI.createURI(localFiles[ATLMRMaster.TRANSFORMATION_ID].toString()), true);
		
		URI inMMURI =URI.createURI( localFiles[ATLMRMaster.SOURCE_METAMODEL_ID].toString());
		
		Metamodel inMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		inMetaModel.setResource(rs.getResource(inMMURI, true));
		executionEnv.registerMetaModel("Families", inMetaModel);
		registerPackages(rs, inMetaModel.getResource());
		logger.log(Level.INFO, "Source metamodel registered");
		URI outMMURI = URI.createURI(localFiles[ATLMRMaster.TARGET_METAMODEL_ID].toString());
		
		Metamodel outMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		outMetaModel.setResource(rs.getResource(outMMURI, true));
		executionEnv.registerMetaModel("Persons", outMetaModel);
		registerPackages(rs, outMetaModel.getResource());
		logger.log(Level.INFO, "Target metamodel registered");
		
		// Load models
		
		URI inMURI = URI.createURI(localFiles[ATLMRMaster.INPUT_MODEL_ID].toString(), true);
		
		inModel = EmftvmFactory.eINSTANCE.createModel();
		inModel.setResource(rs.getResource(inMURI, true));
		executionEnv.registerInputModel("IN", inModel);

		URI outMURI = URI.createFileURI("dataMR/Families2Persons/sample-Persons.out_"+context.getJobID().getId()+".xmi");
		outModel.setResource(rs.createResource(outMURI));
		executionEnv.registerOutputModel("OUT", outModel);
		mr = new DefaultModuleResolver("data/Families2Persons/", rs);
		
		TimingData td = new TimingData();
		executionEnv.loadModule(mr, moduleName);
		td.finishLoading();
		executionEnv.preMatchAllSingle();		
		logger.log(Level.INFO, "Finishing preMatchAll()");
//		try {
//			preApplyAll (context);
//		} catch (IOException | InterruptedException e) {
//			logger.log(Level.SEVERE, "Could not set up traces");
//			e.printStackTrace();
//		} 
		
		logger.log(Level.INFO, "Finishing preApplyAll()");

	}
	
	/*
	 * This method handles one key as moduleName 
	 * 
	 */
//	private void preApplyAll(Context context) throws IOException, InterruptedException {
//		// Parallelize this 
//		Map<String, Object> options = new HashMap<String, Object>();
//		//options.put(XMLResource.OPTION_ENCODING, "UTF-8"); // set encoding to utf-8
//		options.put(XMLResource.OPTION_BINARY, Boolean.TRUE);
//		TraceLinkSet traces =  executionEnv.getTraces();	
//		ByteArrayInputStream bais = null;
//	    //ObjectInputStream ois = null;
//	    while (context.nextKey()) { 
//	    	Iterator<BytesWritable> links = context.getValues().iterator();
//	    	for (ByteArrayInputStream b; links.hasNext(); ) {
//				bais = new ByteArrayInputStream(links.next().getBytes());
//				//Resource resource = new XMIResourceImpl();
//				Resource resource = new BinaryResourceImpl();
//				rs.getResources().add(resource);
////				resource.load(bais, options);
//				resource.load(bais, Collections.emptyMap());
//				mergeTraces (traces, (TracedRule)resource.getContents().get(0));
//				
//			}
//	    }		
//	}

	private void mergeTraces(TraceLinkSet traces, TracedRule tracedRule) {
	    //createElement 
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

	private Path[] getSharedResources(Configuration configuration) {
		
		Path [] result = new Path[4];
		
		result[ATLMRMaster.INPUT_MODEL_ID] = new Path((String)configuration.get(ATLMRMaster.INPUT_MODEL));
		result[ATLMRMaster.SOURCE_METAMODEL_ID] = new Path((String)configuration.get(ATLMRMaster.SOURCE_METAMODEL));
		result[ATLMRMaster.TARGET_METAMODEL_ID] = new Path((String)configuration.get(ATLMRMaster.TARGET_METAMODEL));
		result[ATLMRMaster.TRANSFORMATION_ID] = new Path((String)configuration.get(ATLMRMaster.TRANSFORMATION));
		
		return result;
	}

	private static void registerPackages(ResourceSet rs, Resource resource) {
		
		EObject eObject = resource.getContents().get(0);
		if (eObject instanceof EPackage) {
		    EPackage p = (EPackage)eObject;
		    rs.getPackageRegistry().put(p.getNsURI(), p);
		}	
	  	
	}
}