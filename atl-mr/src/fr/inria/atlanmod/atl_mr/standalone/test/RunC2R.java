package fr.inria.atlanmod.atl_mr.standalone.test;

import java.util.Collections;

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

public class RunC2R {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl());
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("emftvm", new EMFTVMResourceFactoryImpl());
		//		//EmftvmFactory.eINSTANCE;
		//
		ExecEnv env = EmftvmFactory.eINSTANCE.createExecEnv();
		ResourceSet rs = new ResourceSetImpl();
		//
		//		// enable extended metadata
		//		final ExtendedMetaData extendedMetaData = new BasicExtendedMetaData(rs.getPackageRegistry());
		//		rs.getLoadOptions().put(XMLResource.OPTION_EXTENDED_META_DATA,
		//		    extendedMetaData);
		//
		//		Resource emftvmResource = rs.getResource(URI.createURI("data/Families2Persons/Families2Persons.emftvm"), true);
		//		ATLMRUtils.importToXML(emftvmResource);
		//		// Load metamodels
		//		Metamodel metaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		//		metaModel.setResource(rs.getResource(URI.createURI("http://www.eclipse.org/m2m/atl/2011/EMFTVM"), true));
		//		env.registerMetaModel("METAMODEL", metaModel);
		//		registerPackages(rs, metaModel.getResource());
		URI inMMURI =URI.createURI( "./data/Class2Relational/Class.ecore");

		Metamodel inMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		inMetaModel.setResource(rs.getResource(inMMURI, true));
		env.registerMetaModel("Class", inMetaModel);
		registerPackages(rs, inMetaModel.getResource());

		URI outMMURI = URI.createURI("./data/Class2Relational/Relational.ecore");

		Metamodel outMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		outMetaModel.setResource(rs.getResource(outMMURI, true));
		env.registerMetaModel("Relational", outMetaModel);
		registerPackages(rs, outMetaModel.getResource());
		// Load models

		URI inMURI = URI.createURI("./data/Class2Relational/sample.xmi", true);

		Model inModel = EmftvmFactory.eINSTANCE.createModel();
		inModel.setResource(rs.getResource(inMURI, true));
		env.registerInputModel("IN", inModel);

		URI outMURI = URI.createFileURI("./data/Class2Relational/sample.out.xmi");

		Model outModel = EmftvmFactory.eINSTANCE.createModel();
		outModel.setResource(rs.createResource(outMURI));
		env.registerOutputModel("OUT", outModel);

		// Load and run module
		ModuleResolver mr = new DefaultModuleResolver("./data/Class2Relational/", new ResourceSetImpl());
		TimingData td = new TimingData();
		env.loadModule(mr, "Class2Relational");
		td.finishLoading();
		env.run(td);
		td.finish();
		//ATLLogger.info(td.toString());
		// Save models
		outModel.getResource().save(Collections.emptyMap());
	}

	private static void registerPackages(ResourceSet rs, Resource resource) {

		EObject eObject = resource.getContents().get(0);
		if (eObject instanceof EPackage) {
			EPackage p = (EPackage)eObject;
			rs.getPackageRegistry().put(p.getNsURI(), p);
		}
	}


}
