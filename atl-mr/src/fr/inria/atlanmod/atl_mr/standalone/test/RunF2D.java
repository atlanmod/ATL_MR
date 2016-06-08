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

public class RunF2D {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl());
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("emftvm", new EMFTVMResourceFactoryImpl());

		ExecEnv env = EmftvmFactory.eINSTANCE.createExecEnv();
		ResourceSet rs = new ResourceSetImpl();

		URI inMMURI =URI.createURI( "./data/Flow2Data/FlowGraph.ecore");

		Metamodel inMetaModel = EmftvmFactory.eINSTANCE.createMetamodel();
		inMetaModel.setResource(rs.getResource(inMMURI, true));
		env.registerMetaModel("GRP", inMetaModel);
		registerPackages(rs, inMetaModel.getResource());


		URI inMURI = URI.createURI("./data/Flow2Data/ControlFlowGraph-with-Vars.xmi", true);

		Model inModel = EmftvmFactory.eINSTANCE.createModel();
		inModel.setResource(rs.getResource(inMURI, true));
		env.registerInputModel("IN", inModel);

		URI outMURI = URI.createFileURI("./data/Flow2Data/ControlFlowGraph-with-Vars.out.xmi");

		Model outModel = EmftvmFactory.eINSTANCE.createModel();
		outModel.setResource(rs.createResource(outMURI));
		env.registerOutputModel("OUT", outModel);

		// Load and run module

		long tStart = System.currentTimeMillis();
		ModuleResolver mr = new DefaultModuleResolver("./data/Flow2Data/", new ResourceSetImpl());
		TimingData td = new TimingData();
		env.loadModule(mr, "Flow2Data_tvm");
		td.finishLoading();
		env.run(td);
		td.finish();
		// Save models
		outModel.getResource().save(Collections.emptyMap());
		long tEnd = System.currentTimeMillis();
		long tDelta = tEnd - tStart;
		double elapsedSeconds = tDelta / 1000.0;
		System.out.println("Elapsed time in seconds: "+elapsedSeconds);

	}

	private static void registerPackages(ResourceSet rs, Resource resource) {

		EObject eObject = resource.getContents().get(0);
		if (eObject instanceof EPackage) {
			EPackage p = (EPackage)eObject;
			rs.getPackageRegistry().put(p.getNsURI(), p);
		}

	}

}
