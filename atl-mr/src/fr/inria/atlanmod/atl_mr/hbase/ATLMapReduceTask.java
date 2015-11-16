package fr.inria.atlanmod.atl_mr.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.m2m.atl.emftvm.EmftvmFactory;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.ExecMode;
import org.eclipse.m2m.atl.emftvm.Metamodel;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.Module;
import org.eclipse.m2m.atl.emftvm.util.DefaultModuleResolver;
import org.eclipse.m2m.atl.emftvm.util.ModuleResolver;

import fr.inria.atlanmod.atl_mr.utils.ATLMRUtils;
import fr.inria.atlanmod.atl_mr.utils.Tracer;

public class ATLMapReduceTask {

	public static final String TRACES_NSURI = "traces";
	public static final String TRACES_NSURI_MAP = "traces/map";

	private ExecEnv executionEnv;

	private final ResourceSetImpl rs = new ResourceSetImpl();

	private ModuleResolver mr;

	private Model inModel;

	private String moduleName;

	private static Logger logger = Logger.getGlobal();

	private Model outModel;

	private Resource traceResource;

	private Tracer tracer;

	/**
	 * @return the executionEnvironment
	 */
	public ExecEnv getExecutionEnv() {
		return executionEnv;
	}

	/**
	 * @param executionEnv
	 *            the executionEnv to set
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
	 * @param mr
	 *            the mr to set
	 */
	public void setMr(ModuleResolver mr) {
		this.mr = mr;
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
	public Model getOutModel() {
		return outModel;
	}

	/**
	 * @param inModel
	 *            the inModel to set
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
	 * @param moduleName
	 *            the moduleName to set
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

	protected void setup(Configuration configuration, boolean mapState ) {

		// setting up the registry for use
		ATLMRUtils.configureRegistry(configuration);

		URI transformationURI = URI.createURI(configuration.get(ATLMRMaster.TRANSFORMATION));

		String sourceMMpackage = configuration.get(ATLMRMaster.SOURCE_PACKAGE);
		String targetMMpackage = configuration.get(ATLMRMaster.TARGET_PACKAGE);

		URI inMURI = URI.createURI(configuration.get(ATLMRMaster.INPUT_MODEL));
		URI outMURI = URI.createURI(configuration.get(ATLMRMaster.OUTPUT_MODEL));

		// TODO: Check this
		// assuming that module has the same name as the transformation...
		moduleName = resolveModuleName(transformationURI.toString());
		mr = new DefaultModuleResolver(resolveModulePath(transformationURI.toString()), rs);
		Module module = mr.resolveModule(moduleName);

		String SMMNanme = module.getInputModels().get(0).getMetaModelName();
		String IMName = module.getInputModels().get(0).getModelName();
		String TMMName = module.getOutputModels().get(0).getMetaModelName();
		String OMName = module.getOutputModels().get(0).getModelName();

		executionEnv = EmftvmFactory.eINSTANCE.createExecEnv();


		Metamodel inMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		inMetaModel.setResource(rs.createResource(dummyURI(".ecore")));
		inMetaModel.getResource().getContents().add(invokePackageInit(sourceMMpackage));
		ATLMRUtils.registerPackages(rs, inMetaModel.getResource());
		//ATLMRUtils.initPackages(rs);
		Metamodel outMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		outMetaModel.setResource(rs.createResource(dummyURI(".ecore")));
		outMetaModel.getResource().getContents().add(invokePackageInit(targetMMpackage));

		ATLMRUtils.registerPackages(rs, outMetaModel.getResource());

		executionEnv.registerMetaModel(SMMNanme, inMetaModel);
		executionEnv.registerMetaModel(TMMName, outMetaModel);

		executionEnv.loadModule(mr, moduleName);

		// Load models

		inModel = EmftvmFactory.eINSTANCE.createModel();
		inModel.setResource(rs.getResource(inMURI, true));
		executionEnv.registerInputModel(IMName, inModel);

		Resource outResource = rs.createResource(outMURI);
		try {
			outResource.load(Collections.EMPTY_MAP);
		} catch (IOException e) {
			e.printStackTrace();
		}
		outModel = EmftvmFactory.eINSTANCE.createModel();
		outModel.setResource(outResource);

		executionEnv.setExecutionMode(ExecMode.MR);
		executionEnv.registerOutputModel(OMName, outModel);
		executionEnv.preMatchAllSingle();

		setTraceResource(rs.createResource(URI.createURI(inMURI.toString()+"/"+TRACES_NSURI)));
		try {
			getTraceResource().load(Collections.EMPTY_MAP);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private URI dummyURI(String extension) {
		return URI.createURI(UUID.randomUUID().toString()+extension);
	}

	private EObject invokePackageInit(String MMpackageName) {
		EObject _package = null;
		try {
			_package = (EObject)ATLMapReduceTask.class.getClassLoader().loadClass(MMpackageName).getMethod("init").invoke(null);
		} catch (Exception e) {
			ATLMRUtils.showError(e.getLocalizedMessage());
			e.printStackTrace();
		}
		return _package;
	}

	/**
	 * Resolves the Module path from the transformation URI
	 *
	 * @param string
	 * @return module path
	 */
	private String resolveModulePath(String string) {

		String copy = String.copyValueOf(string.toCharArray());
		copy.replaceAll("//", "/");

		return copy.substring(0, copy.lastIndexOf('/') + 1);
	}

	/**
	 * resolves the module name from the transformation URI
	 *
	 * @param string
	 * @return module name
	 */
	private String resolveModuleName(String string) {
		return string.substring(string.lastIndexOf('/') + 1, string.lastIndexOf('.'));
	}

	/**
	 * return the traceLinks resource
	 * @return {@link Resource} LinkSet
	 */
	public Resource getTraceResource() {
		return traceResource;
	}

	/**
	 * sets the trace resource
	 * @param traceResource
	 */
	public void setTraceResource(Resource traceResource) {
		this.traceResource = traceResource;
	}

	/**
	 * gets the tracer
	 * @return
	 */
	public Tracer getTracer() {
		return tracer;
	}

	/**
	 * sets the tracer
	 * @param tracer
	 */
	public void setTracer(Tracer tracer) {
		this.tracer = tracer;
	}

}
