package fr.inria.atlanmod.atl_mr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.BinaryResourceImpl;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.XMLResource;
import org.eclipse.emf.ecore.xmi.impl.EcoreResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceImpl;
import org.eclipse.m2m.atl.emftvm.EmftvmFactory;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.Metamodel;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;
import org.eclipse.m2m.atl.emftvm.util.DefaultModuleResolver;
import org.eclipse.m2m.atl.emftvm.util.ModuleResolver;
import org.eclipse.m2m.atl.emftvm.util.TimingData;


/* 
 * @author Amine BENELALLAM
 * 
 * In this version we are only distributing tasks
 * The number of task is not that big, thus Using IntWritable as a key 
 * 
 * @todo: speciliazing every Text class to a proper specification 
 * example : TransformationText, ModelText
*/

public class ATLMRMapper extends Mapper<LongWritable,Text,Text,BytesWritable> {
	
	private ExecEnv executionEnv;
	
	private ResourceSetImpl rs;
	
	private ModuleResolver mr;

	private Path [] localFiles;
	
	private Model inModel;

	private String moduleName= "Families2Persons";
	
	private static Logger logger = Logger.getGlobal();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Record currentRecord = new Record(value);
		EObject currentObj = inModel.getResource().getEObject(currentRecord.objectFragmentUri);
		Map<String, Object> options = new HashMap<String, Object>();
		options.put(XMLResource.OPTION_ENCODING, "UTF-8"); // set encoding to utf-8
		options.put(XMLResource.OPTION_BINARY, Boolean.TRUE);
		if (executionEnv.matchSingleObject(currentObj, currentRecord.getRuleName())) {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TraceLink currentLink = executionEnv.getCurrentMatch();
        TracedRule currentRule = currentLink.getRule();
        Resource resource = new XMIResourceImpl();
        resource.getContents().addAll(org.eclipse.emf.ecore.util.EcoreUtil.copyAll(currentRule.getLinkSet().eContents()));
        resource.save(baos, options);
        
        //assert currentRule.getLinkSet().eContents().size() == 1: "more than one tracedRule "; 
        //oos.writeObject(currentLink);
		context.write(new Text(moduleName), new BytesWritable(baos.toByteArray()));
		logger.info(String.format("here is the pair key value <%s,%s>", currentRecord.getObjectFragmentUri(), currentRecord.getRuleName()));
//		td.finish();
		}
	}
	
	@Override 
	protected void setup(Context context) {
		
		logger.log(Level.INFO, " Enter map node setup for job "+context.getJobID().getId());
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
		Model outModel = EmftvmFactory.eINSTANCE.createModel();
		outModel.setResource(rs.createResource(outMURI));
		executionEnv.registerOutputModel("OUT", outModel);
		mr = new DefaultModuleResolver("data/Families2Persons/", rs);
		
		TimingData td = new TimingData();
		executionEnv.loadModule(mr, moduleName);
		td.finishLoading();
		executionEnv.preMatchAllSingle();
		
		//moduleName = executionEnv.getModules().get(0).getName();
		
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
		
	private static class Record {
		
		String objectFragmentUri;
		String className;
		
		public Record(Text recordValue) {
			int length = recordValue.getLength();
			int ruleStartIndex = recordValue.toString().indexOf(',');
			objectFragmentUri = recordValue.toString().substring(1, ruleStartIndex);
			className = recordValue.toString().substring(ruleStartIndex+1, length-1);
		}

		public String getObjectFragmentUri() {
			return objectFragmentUri;
		}

		public String getRuleName() {
			return className;
		}
		
	}
}