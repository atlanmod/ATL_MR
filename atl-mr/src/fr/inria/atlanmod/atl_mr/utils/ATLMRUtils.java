package fr.inria.atlanmod.atl_mr.utils;

import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.URIConverter;
import org.eclipse.emf.ecore.resource.impl.BinaryResourceImpl;
import org.eclipse.emf.ecore.resource.impl.ResourceFactoryImpl;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.xmi.XMLResource;
import org.eclipse.emf.ecore.xmi.impl.EcoreResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.URIHandlerImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceImpl;
import org.eclipse.m2m.atl.emftvm.ftrace.FTracePackage;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceImpl;
import org.eclipse.m2m.atl.emftvm.trace.TraceFactory;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TraceLinkSet;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;

import fr.inria.atlanmod.neoemf.core.impl.NeoEMFResourceFactoryImpl;
import fr.inria.atlanmod.neoemf.util.NeoEMFURI;


public class ATLMRUtils {

	public static void showError(String message) {
		System.err.println(message);
	}

	public static String formatMillis(long millis) {
		return String.format("%02d:%02d:%02d",
				TimeUnit.MILLISECONDS.toHours(millis),
				TimeUnit.MILLISECONDS.toMinutes(millis) -
				TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
				TimeUnit.MILLISECONDS.toSeconds(millis) -
				TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));
	}

	public static String byteCountToDisplaySize(long size) {
		int MB = 1024 * 1024;
		return MessageFormat.format("{0} MB", size / MB);
	}

	public static void configureRegistry(final Configuration conf) {
		// Initialize ExtensionToFactoryMap
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("ecore", new EcoreResourceFactoryImpl() {
			@Override
			public Resource createResource(URI uri) {
				XMLResource result = new XMIResourceImpl(uri) {
					@Override
					protected boolean useIDs() {
						return eObjectToIDMap != null || idToEObjectMap != null;
					}

					@Override
					protected URIConverter getURIConverter() {
						return new HadoopURIConverterImpl(conf);
					}
				};
				result.setEncoding("UTF-8");

				result.getDefaultSaveOptions().put(XMLResource.OPTION_USE_ENCODED_ATTRIBUTE_STYLE, Boolean.TRUE);
				result.getDefaultSaveOptions().put(XMLResource.OPTION_LINE_WIDTH, 80);
				result.getDefaultSaveOptions().put(XMLResource.OPTION_URI_HANDLER, new URIHandlerImpl.PlatformSchemeAware());
				return result;
			}
		});
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl() {
			@Override
			public Resource createResource(URI uri) {
				return new XMIResourceImpl(uri) {
					@Override
					protected URIConverter getURIConverter() {
						return new HadoopURIConverterImpl(conf);
					}
				};
			}
		});

		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("bin", new ResourceFactoryImpl() {
			@Override
			public Resource createResource(URI uri) {
				return new BinaryResourceImpl(uri) {
					@Override
					protected URIConverter getURIConverter() {
						return new HadoopURIConverterImpl(conf);
					}
				};
			}
		});

		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("emftvm", new EMFTVMResourceFactoryImpl() {
			@Override
			public Resource createResource(URI uri) {
				return new EMFTVMResourceImpl(uri) {
					@Override
					protected URIConverter getURIConverter() {
						return new HadoopURIConverterImpl(conf);
					}
				};
			}
		});

		Resource.Factory.Registry.INSTANCE.getProtocolToFactoryMap().put(NeoEMFURI.NEOEMF_HBASE_SCHEME, new NeoEMFResourceFactoryImpl());

		@SuppressWarnings("unused")
		EPackage ftrace = FTracePackage.eINSTANCE;
	}


	//	/**
	//	 * This method is used to extract rules from a
	//	 * @param resource
	//	 * @return
	//	 * @throws IOException
	//	 */
	//	public static HashMap<String, String> extractRules(Resource resource) throws IOException {
	//
	//		HashMap<String, String> map = new HashMap<String, String>();
	//		if (!resource.isLoaded()) {
	//			resource.load(null);
	//		}
	//
	//		Iterator<EObject> iterator = resource.getAllContents();
	//		EObject objectIterator = null;
	//		while (iterator.hasNext()) {
	//			objectIterator = iterator.next();
	//			if (objectIterator instanceof Rule) {
	//				Rule rule = ((Rule) objectIterator);
	//				map.put(rule.getInputElements().get(0).getEType().getName(), rule.getName());
	//
	//			}
	//		}
	//		return map;
	//
	//	}

	/**
	 * Registers the packages
	 * @param resourceSet
	 * @param resource
	 */
	public static void registerPackages(ResourceSet resourceSet, Resource resource) {
		EObject eObject = resource.getContents().get(0);
		if (eObject instanceof EPackage) {
			EPackage p = (EPackage) eObject;
			resourceSet.getPackageRegistry().put(p.getNsURI(), p);
		}

	}

	/**
	 * Copies a rule in order to be serialized and passed to the reduced
	 * @param currentRule
	 * @param currentLink
	 * @return
	 */
	public static TracedRule copyRule(TracedRule currentRule, TraceLink currentLink) {
		TraceLinkSet set = TraceFactory.eINSTANCE.createTraceLinkSet();
		TracedRule tracedRule = TraceFactory.eINSTANCE.createTracedRule();
		TraceLink newLink = EcoreUtil.copy(currentLink);
		tracedRule.setLinkSet(set);
		tracedRule.setRule(currentRule.getRule());
		newLink.setRule(tracedRule);
		return tracedRule;
	}
}
