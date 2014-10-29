package fr.inria.atlanmod.atl_mr;

import java.util.logging.Logger;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
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
import org.eclipse.m2m.atl.emftvm.Module;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;
import org.eclipse.m2m.atl.emftvm.util.DefaultModuleResolver;
import org.eclipse.m2m.atl.emftvm.util.ModuleResolver;
import org.eclipse.m2m.atl.emftvm.util.TimingData;
import fr.inria.atlanmod.atl_mr.utils.ATLMRUtils;

public class ATLMapReduceTask {

	private ExecEnv executionEnv;
	
	private final ResourceSetImpl rs = new ResourceSetImpl();
	
	private ModuleResolver mr;

	private URI [] localFiles;
	
	private Model inModel;

	private String moduleName= "Families2Persons";
	
	private static Logger logger = Logger.getGlobal();
	
	private Model outModel;

	/**
	 * @return the executionEnvironment
	 */
	public ExecEnv getExecutionEnv() {
		return executionEnv;
	}

	/**
	 * @param executionEnv the executionEnv to set
	 */
	public void setExecutionEnv(ExecEnv executionEnv) {
		this.executionEnv = executionEnv;
	}

	/**
	 * @return the rs
	 */
	public ResourceSetImpl getRs() {
		return rs;
	}


	/**
	 * @return the mr
	 */
	public ModuleResolver getMr() {
		return mr;
	}

	/**
	 * @param mr the mr to set
	 */
	public void setMr(ModuleResolver mr) {
		this.mr = mr;
	}

	/**
	 * @return the localFiles
	 */
	public URI[] getLocalFiles() {
		return localFiles;
	}

	/**
	 * @param localFiles the localFiles to set
	 */
	public void setLocalFiles(URI[] localFiles) {
		this.localFiles = localFiles;
	}

	/**
	 * @return the inModel
	 */
	public Model getInModel() {
		return inModel;
	}
	/**
	 * 
	 * @return the outModel
	 */
	public Model getOutModel(){
		return outModel;
	}
	/**
	 * @param inModel the inModel to set
	 */
	public void setInModel(Model inModel) {
		this.inModel = inModel;
	}

	/**
	 * @return the moduleName
	 */
	public String getModuleName() {
		return moduleName;
	}

	/**
	 * @param moduleName the moduleName to set
	 */
	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}

	/**
	 * @return the logger
	 */
	public static Logger getLogger() {
		return logger;
	}



	protected void setup(Configuration configuration) {
		// Resolving the module 
		localFiles = getSharedResources(configuration);
		
		URI outMURI = localFiles[ATLMRMaster.OUTPUT_MODEL_ID] == null ?
				URI.createFileURI(ATLMRUtils.resolveOutputPath(localFiles[ATLMRMaster.INPUT_MODEL_ID].toString())) :
					localFiles[ATLMRMaster.OUTPUT_MODEL_ID];
		URI inMURI = localFiles[ATLMRMaster.INPUT_MODEL_ID];		
		URI sourceMMURI =  localFiles[ATLMRMaster.SOURCE_METAMODEL_ID];
		URI targetMMURI = localFiles[ATLMRMaster.TARGET_METAMODEL_ID];
		URI transformationURI = localFiles[ATLMRMaster.TRANSFORMATION_ID];
		
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl());
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());	
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("emftvm", new EMFTVMResourceFactoryImpl());
		
		moduleName = resolveModuleName(transformationURI.toString());
		mr = new DefaultModuleResolver(resolveModulePath(transformationURI.toString()), rs);
		Module module = mr.resolveModule(moduleName);
		
		
		String SMMNanme = module.getInputModels().get(0).getMetaModelName();
		String IMName = module.getInputModels().get(0).getModelName();
		String TMMName = module.getOutputModels().get(0).getMetaModelName();
		String OMName = module.getOutputModels().get(0).getModelName();
		
		executionEnv = EmftvmFactory.eINSTANCE.createExecEnv();
		// assuming that module has the same name as the transformation ....
		
		Metamodel inMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		inMetaModel.setResource(rs.getResource(sourceMMURI, true));
		
		ATLMRUtils.registerPackages(rs, inMetaModel.getResource());
		
		//URI outMMURI = URI.createURI(localFiles[ATLMRMaster.TARGET_METAMODEL_ID].toString());	
		Metamodel outMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		outMetaModel.setResource(rs.getResource(targetMMURI, true));
		
		ATLMRUtils.registerPackages(rs, outMetaModel.getResource());
		
		executionEnv.registerMetaModel(SMMNanme, inMetaModel); 
		executionEnv.registerMetaModel(TMMName, outMetaModel);
		
		executionEnv.loadModule(mr, moduleName);
		
		// Load models
		
		
		inModel = EmftvmFactory.eINSTANCE.createModel();
		inModel.setResource(rs.getResource(inMURI, true));
		executionEnv.registerInputModel(IMName, inModel);

		
			
		outModel = EmftvmFactory.eINSTANCE.createModel();
		outModel.setResource(rs.createResource(outMURI));
		executionEnv.registerOutputModel(OMName, outModel);
		executionEnv.preMatchAllSingle();
		
		//moduleName = executionEnv.getModules().get(0).getName();
		
	}
	
	/**
	 * Resolves the Module path from the transformation URI
	 * @param string
	 * @return module path
	 */
	private String resolveModulePath(String string) {
		String copy = String.copyValueOf(string.toCharArray());
		copy.replaceAll("//", "/");
		
		return copy.substring(0, copy.lastIndexOf('/')+1);
	}

	/**
	 *  Resolves the module name from the transformation URI
	 * @param string
	 * @return module name 
	 */
	private String resolveModuleName(String string) {
		return string.substring(string.lastIndexOf('/')+1, string.lastIndexOf('.'));
	}

	
	private URI[] getSharedResources(Configuration configuration) {
		
		URI [] result = new URI[5];
		
		result[ATLMRMaster.INPUT_MODEL_ID] = URI.createURI((String)configuration.get(ATLMRMaster.INPUT_MODEL));
		result[ATLMRMaster.SOURCE_METAMODEL_ID] = URI.createURI((String)configuration.get(ATLMRMaster.SOURCE_METAMODEL));
		result[ATLMRMaster.TARGET_METAMODEL_ID] = URI.createURI((String)configuration.get(ATLMRMaster.TARGET_METAMODEL));
		result[ATLMRMaster.TRANSFORMATION_ID] = URI.createURI((String)configuration.get(ATLMRMaster.TRANSFORMATION));
		try {
			result[ATLMRMaster.OUTPUT_MODEL_ID] = URI.createURI((String)configuration.get(ATLMRMaster.OUTPUT_MODEL));
			} catch (Exception e) {
				// continue
			}
		
		
		return result;
	}
}
