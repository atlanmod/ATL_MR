package fr.inria.atlanmod.atl_mr.hbase.splitter;

import java.util.LinkedList;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;

import fr.atlanmod.class_.classPackage;
import fr.atlanmod.class_.impl.classPackageImpl;


public class GreedySplitterC2R extends GreedyDeterministicSplitter {

	// TOBE used for dynamic initialization of the classPackage
	// private static final String CLASS_PACKAGE_NAME	 = "fr.atlanmod.class_.impl.classPackageImpl";

	public GreedySplitterC2R(URI tableURI, long bufferCapacity) {
		super(tableURI, bufferCapacity);

		classPackageImpl.init();
		// rule package footprints
		LinkedList<CallSite> packageFootprints = new LinkedList<AbstractSplitter.CallSite>();

		EClass pck = classPackage.Literals.PACKAGE;
		footprints.put(pck.getName(), packageFootprints);
		{
			packageFootprints.add(new CallSite(classPackage.Literals.PACKAGE__CLASSES, pck));
			packageFootprints.add(new CallSite(classPackage.Literals.PACKAGE__TYPES, pck));
		}
		// rule class footprints
		LinkedList<CallSite> classFootprints = new LinkedList<AbstractSplitter.CallSite>();
		EClass clazz = classPackage.Literals.CLASS;
		footprints.put(clazz.getName(), classFootprints);
		{
			packageFootprints.add(new CallSite(classPackage.Literals.CLASS__ATTR, clazz));
		}

		// rule attribute footprint
		LinkedList<CallSite> attributeFootprints = new LinkedList<AbstractSplitter.CallSite>();
		EClass attribute = classPackage.Literals.ATTRIBUTE;
		footprints.put(attribute.getName(), attributeFootprints);
		{
			attributeFootprints.add(new CallSite(classPackage.Literals.ATTRIBUTE__TYPE, attribute));
			attributeFootprints.add(new CallSite(classPackage.Literals.ATTRIBUTE__OWNER, attribute));
		}

		// rule datatype footprint
		//		LinkedList<CallSite> dataFootprints = new LinkedList<AbstractSplitter.CallSite>();
		//		EClass type = classPackage.Literals.DATA_TYPE;

	}

	@Override
	protected boolean isHighLevel(byte[] row) {
		return ! resolveInstanceOf(row).getName().equals(classPackage.Literals.DATA_TYPE.getName());
	}
}
