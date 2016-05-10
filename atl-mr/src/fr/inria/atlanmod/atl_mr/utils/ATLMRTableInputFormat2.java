package fr.inria.atlanmod.atl_mr.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.NamingException;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import fr.inria.atlanmod.atl_mr.hbase.ATLMRHBaseMaster;

/**
 * Custom implementation of input data for NeoEMF models
 * @author Amine BENELALLAM
 *
 */
public class ATLMRTableInputFormat2 extends TableInputFormat {


	public ATLMRTableInputFormat2() {
		super();
	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Logger log = Logger.getGlobal();
		HTable table = getHTable();
		if (getHTable() == null) {
			throw new IOException("No table was provided.");
		}

		Pair<byte[][],byte[][]> keys=table.getStartEndKeys();

		if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
			if (keys == null) {
				log.log(Level.SEVERE, "keys are null");
			} else if (keys.getFirst() == null) {
				log.log(Level.SEVERE, "first key is null");
			} else {
				log.log(Level.SEVERE, "the lenght of first key is null");
			}

			throw new IOException("Expecting at least one region for table: "+ getHTable().getName().getNameAsString());
		}

		List<InputSplit> splits = super.getSplits(job);
		int rec_map = Integer.valueOf(job.getConfiguration().get(ATLMRHBaseMaster.RECOMMENDED_MAPPERS));

		if (rec_map <= splits.size()) {
			return splits;
		}

		List<InputSplit> newSplits = new LinkedList<InputSplit>();
		RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(table);
		Scan myScan = getScan();

		// splitting the current
		try {

			long rowCount = new AggregationClient(job.getConfiguration()).rowCount(getHTable(), new LongColumnInterpreter() , myScan);
			int rowsPerSplit = Math.round(rowCount/rec_map) + 1;
			int subSplitsCount = Math.round(rec_map/splits.size());
			//ResultScanner scanner = table.getScanner(myScan);
			for (InputSplit split : splits) {
				int i =splits.indexOf(split);
				//	RecordReader<ImmutableBytesWritable, Result> rr = createRecordReader(split, job);
				HRegionLocation location = table.getRegionLocation(keys.getFirst()[i], false);
				// The below InetSocketAddr.getCurrentKey().ess creation does a name resolution.
				InetSocketAddress isa = new InetSocketAddress(location.getHostname(), location.getPort());
				InetAddress regionAddress = isa.getAddress();
				String regionLocation;
				try {
					regionLocation = reverseDNS(regionAddress);
				} catch (NamingException e) {
					regionLocation = location.getHostname();
				}

				myScan.setStartRow(keys.getFirst()[i]);
				myScan.setStopRow(keys.getSecond()[i]);
				ResultScanner subScanner = table.getScanner(myScan);
				byte[] regionName = location.getRegionInfo().getRegionName();
				long regionSize = sizeCalculator.getRegionSize(regionName);
				newSplits.addAll(
						subSplit(
								subScanner,
								table.getName(),
								keys.getFirst()[i],
								keys.getSecond()[i],
								regionLocation,
								regionSize,
								subSplitsCount,
								rowsPerSplit));
			}
			return newSplits;

		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;

	}


	/**
	 * @param subScanner
	 * @param tableName
	 * @param startRowKey
	 * @param endRowKey
	 * @param regionLocation
	 * @param regionSize
	 * @param subSplitsCount
	 * @param rowsPerSplit
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */

	private Collection<? extends InputSplit> subSplit(
			ResultScanner subScanner,
			TableName tableName,
			byte[] startRowKey,
			byte[] endRowKey,
			String regionLocation,
			long regionSize,
			int subSplitsCount,
			int rowsPerSplit) throws IOException, InterruptedException {


		List<InputSplit> splits = new LinkedList<InputSplit>();
		Result result;
		byte[] intermediateRow=null;

		for (int sub = 0; sub < subSplitsCount -1; sub++) {

			for ( int i = 0; i < rowsPerSplit-1; i++) {
				result = subScanner.next();

				if (result != null) {
					intermediateRow = result.getRow();
				} else {
					splits.add(new TableSplit(tableName, startRowKey, endRowKey, regionLocation,regionSize));
					return splits;
				}

				if (Bytes.compareTo(intermediateRow, endRowKey) == 0) {
					splits.add(new TableSplit(tableName, startRowKey, endRowKey, regionLocation,regionSize));
					return splits;
				}
			}

			// adding last split
			splits.add(new TableSplit(tableName,
					startRowKey,
					intermediateRow,
					regionLocation,
					regionSize));

			result = subScanner.next();
			if (result == null) {
				return splits;
			}
			startRowKey = result.getRow();
		}
		splits.add(new TableSplit(tableName,
				startRowKey,
				endRowKey,
				regionLocation,
				regionSize));
		return splits;
	}
}