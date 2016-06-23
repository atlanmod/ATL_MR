package fr.inria.atlanmod.atl_mr.hbase.splitter;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.hbase.client.Result;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EStructuralFeature;

public class GreedyDeterministicSplitter extends AbstractSplitter {

	protected static final long DEFAULT_BUFFER_CAPACITY = 100L;

	protected long bufferCapacity = DEFAULT_BUFFER_CAPACITY;

	protected long avgSize;

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

		for ( Result result : sourceTable.getScanner(scan)) {
			//result.get
			byte[] row = result.getRow();
			if(! isHighLevel(row)) {
				buffer.add(row);
				sizeBuff++;
			} else {
				while (!buffer.isEmpty()) {
					assignElement (buffer.remove(), buffer, avgSize);
				}
			}

			if (sizeBuff == bufferCapacity) { // clean the buffer if it reaches the capacity
				while (!buffer.isEmpty()) {
					assignElement (buffer.remove(), buffer, avgSize);
				}
				sizeBuff =0;
			}
		}
		// clean the buffer at the end
		while (!buffer.isEmpty()) {
			assignElement (buffer.remove(), buffer, avgSize);
		}
		sizeBuff =0;

	}

	protected int assignElement(byte[] row, Queue<byte[]> buffer, double avgSize) {

		int splitId = getConvenientSplit(row);

		updateDependencyCache(row, splitId);

		addSaltedToTable(row, splitId);

		return splitId;
	}

	protected void addSaltedToTable(byte[] row, int splitId) {
		// TODO Auto-generated method stub
	}

	protected void updateDependencyCache(byte[] row, int splitId) {
		String clazz = resolveInstanceNameOf(row);

		for (CallSite cs : footprints.get(clazz)) {
			EStructuralFeature feat = cs.getFeature();

			if (! feat.isMany()) {

			} else {

			}
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

		// update the splits length
		splitsLength[index]++;
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
