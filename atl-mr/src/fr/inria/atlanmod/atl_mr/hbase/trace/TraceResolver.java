package fr.inria.atlanmod.atl_mr.hbase.trace;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.ResourceSet;

abstract class TraceResolver {

	/**
	 * the resource set containing sources and targets
	 */
	protected ResourceSet rs;

	/**
	 * a string path to the traces location
	 */
	protected String tracesPath;

	/**
	 * returns the the corresponding element at location i
	 * behaves like resolveTemp (x, index (input-pattern))
	 * @param objectURI
	 * @return
	 */
	public abstract EObject resolveSourceElement (URI objectURI, int i);

	/**
	 * resolves the first object
	 * behaves like a normal resolve
	 * @param objectURI
	 * @return
	 */
	public EObject resolveEObject (URI objectURI) {
		return resolveSourceElement(objectURI, 0);
	}

	/**
	 * Cleaning the traces resource at the end of the creation
	 */
	public abstract boolean cleanTraces() ;
}