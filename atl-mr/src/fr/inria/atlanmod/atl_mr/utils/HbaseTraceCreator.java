package fr.inria.atlanmod.atl_mr.utils;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.emf.common.util.URI;

import fr.inria.atlanmod.atl_mr.utils.Tracer.Creator;

public class HbaseTraceCreator extends HbaseTracer implements Creator {

	/**
	 *public constructor
	 */
	public HbaseTraceCreator(URI traceURI) {
		super( traceURI);
	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean addMapping(String source, String[] targets) {

		Put put = new Put(Bytes.toBytes(source.toString()));
		put.add(EINVERSE_FAMILY, TARGET_COLUMN, toPrettyBytes(targets));
		try {
			table.put(put);
			return true;
		} catch (IOException e) {

			e.printStackTrace();
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean addMapping(String source, String target) {

		return addMapping(source, new String[] {target});
	}

}
