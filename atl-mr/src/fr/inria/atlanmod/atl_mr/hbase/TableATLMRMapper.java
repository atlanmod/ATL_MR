package fr.inria.atlanmod.atl_mr.hbase;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.ExecPhase;
import org.eclipse.m2m.atl.emftvm.Model;

import fr.inria.atlanmod.atl_mr.utils.HbaseTraceCreator;

public class TableATLMRMapper extends TableMapper<LongWritable, Text> {

	private ATLMapReduceTask mapTask = new ATLMapReduceTask();
	//private BinaryResourceImpl tracesResource = new BinaryResourceImpl();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Logger.getGlobal().log(Level.INFO, "Setting up mapper - START");

		try {
			mapTask.setup(context.getConfiguration(), true);
			mapTask.setTracer(new HbaseTraceCreator(mapTask.getTraceResource().getURI()));
		} catch (Exception e) {
			e.printStackTrace();
		}

		mapTask.getExecutionEnv().setExecutionPhase(ExecPhase.PRE);
		Logger.getGlobal().log(Level.INFO, "Setting up mapper - END");
	};

	@Override
	public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		Logger.getGlobal().log(Level.FINEST, "Mapping - START - " + value.toString());

		try {
			Model inModel = mapTask.getInModel();
			ExecEnv executionEnv = mapTask.getExecutionEnv();
			String objId = Bytes.toString(row.get());
			EObject currentObj = inModel.getResource().getEObject(objId);
			System.out.println("****"+currentObj+"****");
			Logger.getGlobal().log(Level.FINEST, "\tmatchSingleObject - START");

			System.out.println(executionEnv.matchSingleObject(currentObj, currentObj.eClass().getName()));
			Logger.getGlobal().log(Level.FINEST, "\tmatchSingleObject - END");

			Logger.getGlobal().log(Level.FINEST, "Mapping - END");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
