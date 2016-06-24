package fr.inria.atlanmod.atl_mr.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import fr.inria.atlanmod.atl_mr.utils.ATLMRUtils;

public class TableATLMRGreedyMapper extends TableATLMRMapper {


	// in this implementation we scan values not keys
	// override it for now

	@Override
	protected String getObjectId(ImmutableBytesWritable row) {
		return Bytes.toString(ATLMRUtils.unsaltId(row.get()));
	}
}
