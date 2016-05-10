package fr.inria.atlanmod.atl_mr.utils;

import org.eclipse.emf.ecore.EObject;

public interface Tracer {

	public interface Creator {

		/**
		 * maps a source element to a list of target
		 * @param source
		 * @param targets
		 * @return
		 */
		public boolean addMapping (String source, String [] targets);
		/**
		 * maps a source element to a single target element
		 * @param source
		 * @param target
		 * @return
		 */
		public boolean addMapping (String source, String target);
	}

	public interface Resolver {
		/**
		 * returns the the corresponding element at location i
		 * behaves like resolveTemp (x, outputPatternAtIndex(i))
		 * @param objectString
		 * @return
		 */
		public EObject resolve (String sourceElement, int index );
		/**
		 * resolves the first object
		 * behaves like a normal resolve
		 * @param objectString
		 * @return
		 */
		public EObject resolve (String sourceElement);

	}
}
