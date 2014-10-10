package fr.inria.atlanmod.atl_mr;

import java.io.IOException;













import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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

public class ATLMRMapper extends Mapper<Text,Text,Text,Text> {
	
	private ExecEnv executionEnv;
	
	private ResourceSetImpl rs;
	
	private ModuleResolver mr;

	private Path [] localFiles;
	
	private final int ATL_TRANSFORAMTION=0;
	
	private final int INPUT_METAMODEL=1;
	
	private final int OUTPUT_METAMODEL=2;
	
	private final int INPUT_MODEL=3;
	
	private Model outModel;

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("Mapper, the job ID is : "+context.getJobID().getId());
		TimingData td = new TimingData();
		executionEnv.loadModule(mr, "Families2Persons");
		td.finishLoading();
		executionEnv.run(td);
		td.finish();
	}
	
	@Override 
	protected void setup(Context context) {
		
		try {
			
			localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl());
			Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());	
			Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("emftvm", new EMFTVMResourceFactoryImpl());
			
			executionEnv = EmftvmFactory.eINSTANCE.createExecEnv();
			rs = new ResourceSetImpl();
			
			Resource emftvmResource = rs.getResource(URI.createURI(localFiles[ATL_TRANSFORAMTION].toString()), true);
			
			URI inMMURI =URI.createURI( localFiles[INPUT_METAMODEL].toString());
			
			Metamodel inMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
			inMetaModel.setResource(rs.getResource(inMMURI, true));
			executionEnv.registerMetaModel("Families", inMetaModel);
			registerPackages(rs, inMetaModel.getResource());
			
			URI outMMURI = URI.createURI(localFiles[OUTPUT_METAMODEL].toString());
			
			Metamodel outMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
			outMetaModel.setResource(rs.getResource(outMMURI, true));
			executionEnv.registerMetaModel("Persons", outMetaModel);
			registerPackages(rs, outMetaModel.getResource());
			// Load models
			
			URI inMURI = URI.createURI(localFiles[INPUT_MODEL].toString(), true);
			
			Model inModel = EmftvmFactory.eINSTANCE.createModel();
			inModel.setResource(rs.getResource(inMURI, true));
			executionEnv.registerInputModel("IN", inModel);

			URI outMURI = URI.createFileURI("dataMR/Families2Persons/sample-Persons.out_"+context.getJobID().getId()+".xmi");
			outModel = EmftvmFactory.eINSTANCE.createModel();
			outModel.setResource(rs.createResource(outMURI));
			executionEnv.registerOutputModel("OUT", outModel);
			mr = new DefaultModuleResolver("data/Families2Persons/", rs);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void registerPackages(ResourceSet rs, Resource resource) {
		
		EObject eObject = resource.getContents().get(0);
		if (eObject instanceof EPackage) {
		    EPackage p = (EPackage)eObject;
		    rs.getPackageRegistry().put(p.getNsURI(), p);
		}		
	}
	
}