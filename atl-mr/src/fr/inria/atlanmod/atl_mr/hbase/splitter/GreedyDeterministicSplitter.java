package fr.inria.atlanmod.atl_mr.hbase.splitter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EStructuralFeature;

import fr.inria.atlanmod.neoemf.util.NeoEMFUtil;

public class GreedyDeterministicSplitter extends AbstractSplitter {

	protected static final long DEFAULT_BUFFER_CAPACITY = 100L;

	protected long bufferCapacity = DEFAULT_BUFFER_CAPACITY;

	protected long avgSize;

	protected long [] splitsLength;

	public GreedyDeterministicSplitter(URI tableURI) {
		super(tableURI);
	}

	public GreedyDeterministicSplitter(URI tableURI, long bufferCapacity) {
		this(tableURI);
		this.bufferCapacity = bufferCapacity;
	}

	@Override
	void split(int splits) throws Exception {
		Queue<byte[]> buffer = new LinkedList<byte[]>();
		long sizeBuff = 0;
		avgSize = Math.abs((modelSize/splits));
		splitsLength = new long [splits];
		for ( Result result : sourceTable.getScanner(scan)) {
			//result.get
			byte[] row = result.getRow();
			if(! isHighLevel(row)) {
				buffer.add(row);
				sizeBuff++;
			} else {
				assignElement(row, buffer);
			}

			if (sizeBuff == bufferCapacity) { // clean the buffer if it reaches the capacity
				while (!buffer.isEmpty()) {
					assignElement (buffer.remove(), buffer);
				}
				sizeBuff =0;
			}
		}
		// clean the buffer at the end
		while (!buffer.isEmpty()) {
			assignElement (buffer.remove(), buffer);
		}
		sizeBuff =0;

	}

	protected int splitIndexWithMinsize() {

		int index = 0;
		long min = splitsLength [0];

		for (int i = 1 ; i < splitsLength.length; i++ ){
			if (splitsLength [i] < min ) {
				min = splitsLength [i];
				index = i;
			}
		}

		return index;
	}
	protected int assignElement(byte[] row, Queue<byte[]> buffer) {

		int splitId = -1;
		Result result;
		try {
			// computing the split that fits this value the most
			// the heuristic is based on  the dependency map
			if (dependencyMap.containsKey(row)) {
				// returns the index of the most fitting split
				splitId =  indexMaxDeps(dependencyMap.get(row));
			} else {
				// returns the split with the minimum size
				splitId = splitIndexWithMinsize();
			}

			// updating the dependency cache
			String clazz = resolveInstanceNameOf(row);
			result= sourceTable.get(new Get(row));
			for (CallSite cs : footprints.get(clazz)) {
				EStructuralFeature feat = cs.getFeature();
				byte[] value = result.getValue(PROPERTY_FAMILY, Bytes.toBytes(feat.getName()));

				if (! feat.isMany()) {
					updateSingleDependency(value, splitId);
				} else {
					for (String singleValue : NeoEMFUtil.EncoderUtil.toStringsReferences(value)) {
						updateSingleDependency(Bytes.toBytes(singleValue), splitId);
					}
				}
			}

			// Adding the salted elements to target table

			Put put = new Put(salted(row,splitId));
			put.add(ORIGINAL_ID_FAMILY, ORIGINAL_ID_COLUMN, row);
			saltedTable.put(put);

			// update the splits length
			splitsLength[splitId]++;

			// TODO remove already assigned element if exist
			if (dependencyMap.containsKey(row)) {
				dependencyMap.remove(row);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return splitId;
	}

	private byte[] salted(byte[] row, int splitId) {
		return Bytes.toBytes(String.format("%02d", splitId) + Bytes.toString(row));
	}

	protected void addSaltedToTable(byte[] row, int splitId) {
		// TODO Auto-generated method stub
	}


	private void updateSingleDependency(byte[] value, int splitId) {
		if (dependencyMap.containsKey(value)) {
			dependencyMap.get(value).incrementSplit(splitId);
		} else {
			SimpleDepsList list = new SimpleDepsList(splitsLength.length);
			list.incrementSplit(splitId);
			dependencyMap.put(value, list);
		}
	}

	protected int getConvenientSplit(byte[] row) {
		if (dependencyMap.containsKey(row)) {
			return indexMaxDeps(dependencyMap.get(row));
		} else {
			return splitIndexWithMinsize();
		}
	}

	protected int indexMaxDeps(SimpleDepsList dependencies) {
		long [] depsCount  =  dependencies.getDependecyCount();
		int index = 0;
		long max_score = 0;

		for (int i =0; i < depsCount.length; i++) {
			if (scoreSplit(i, depsCount[i])  > max_score) {
				max_score = scoreSplit(i, depsCount[i]);
				index = i;
			}
		}

		return index;
	}

	protected long scoreSplit(int cluster, long depsCount) {
		return depsCount * (1 - (splitsLength [cluster] / (modelSize /avgSize)));
	}

	@Override
	protected boolean isHighLevel(byte[] row) {
		// TODO implement this according to the higher priorities of elements distribution
		// for C2S it would be discarding data types or packages
		return true;
	}


}
