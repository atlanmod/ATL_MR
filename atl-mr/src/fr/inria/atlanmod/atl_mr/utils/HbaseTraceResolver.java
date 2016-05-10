package fr.inria.atlanmod.atl_mr.utils;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;



public class HbaseTraceResolver extends HbaseTracer implements Tracer.Resolver{

	/**
	 * the target resource to be resolving from
	 */
	protected Resource resource;

	public HbaseTraceResolver( URI traceURI) {
		super(traceURI);
	}
	/**
	 * public constructor
	 * @param traceURI
	 * @param resource
	 */
	public HbaseTraceResolver(URI traceURI, Resource resource) {

		super(traceURI);
		this.resource = resource;

	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public EObject resolve(String sourceElement, int index) {

		try {
			Result result;
			result = table.get(new Get(Bytes.toBytes(sourceElement)));
			if (result.getValue(EINVERSE_FAMILY, TARGET_COLUMN) == null) {
				return null;
			}
			String targetId = toPrettyStrings(result.getValue(EINVERSE_FAMILY, TARGET_COLUMN))[0];
			return resource.getEObject(targetId);


		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public EObject resolve(String sourceElement) {
		return resolve(sourceElement, 0);
	}


}
