package fr.inria.atlanmod.atl_mr;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.impl.EcoreResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.EmftvmFactory;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.Metamodel;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;
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

public class ATLMRMapper extends Mapper<LongWritable,Text,Text,Text> {
	
	private ExecEnv executionEnv;
	
	private ResourceSetImpl rs;
	
	private ModuleResolver mr;

	private Path [] localFiles;
	
	private Model outModel;
	
	private static Logger logger = Logger.getGlobal();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("#########################################\n"
				+ "Mapper, the job ID is :"
				+ context.getJobID().getId()
				+ "\n#####################################  ");
		TimingData td = new TimingData();
		executionEnv.loadModule(mr, "Families2Persons");
		td.finishLoading();
		executionEnv.run(td);
		td.finish();
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
		
		Resource emftvmResource = rs.getResource(URI.createURI(localFiles[ATLMRMaster.TRANSFORMATION_ID].toString()), true);
		
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
		
		Model inModel = EmftvmFactory.eINSTANCE.createModel();
		inModel.setResource(rs.getResource(inMURI, true));
		executionEnv.registerInputModel("IN", inModel);

		URI outMURI = URI.createFileURI("dataMR/Families2Persons/sample-Persons.out_"+context.getJobID().getId()+".xmi");
		outModel = EmftvmFactory.eINSTANCE.createModel();
		outModel.setResource(rs.createResource(outMURI));
		executionEnv.registerOutputModel("OUT", outModel);
		mr = new DefaultModuleResolver("data/Families2Persons/", rs);
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