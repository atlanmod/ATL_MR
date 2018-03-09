package fr.inria.atlanmod.atl_mr.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.ExecPhase;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.trace.TargetElement;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;

import fr.inria.atlanmod.atl_mr.hbase.trace.HbaseTraceCreator;
import fr.inria.atlanmod.atl_mr.hbase.trace.Tracer;
import fr.inria.atlanmod.atl_mr.hbase.trace.Tracer.Creator;
import fr.inria.atlanmod.neoemf.core.NeoEMFEObject;

public class TableATLMRMapper extends TableMapper<LongWritable, Text> {

	protected ATLMapReduceTask mapTask = new ATLMapReduceTask();
	protected Random randomGen =  new Random();

	//private BinaryResourceImpl tracesResource = new BinaryResourceImpl();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Logger.getGlobal().log(Level.INFO, "Setting up mapper - START");

		try {
			mapTask.setContext(context);
			mapTask.setup(context.getConfiguration(), true);
			mapTask.setTracer(new HbaseTraceCreator(mapTask.getInModel().
					getResource().getURI().
					appendSegment("traces"))
					);
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
			String objId = getObjectId(row);

			EObject currentObj = mapTask.getInModel().getResource().getEObject(objId);
			//System.out.println("****"+currentObj+"****");
			Logger.getGlobal().log(Level.FINEST, "\tmatchSingleObject - START");

			mapTask.getExecutionEnv().matchSingleObject(currentObj, currentObj.eClass().getName());
			Logger.getGlobal().log(Level.FINEST, "\tmatchSingleObject - END");

			Logger.getGlobal().log(Level.FINEST, "Mapping - END");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected String getObjectId(ImmutableBytesWritable row) {
		return Bytes.toString(row.get());
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		ExecEnv execEnv = mapTask.getExecutionEnv();
		Tracer.Creator traceCreator = (Creator) mapTask.getTracer();
		Logger.getGlobal().log(Level.INFO, "Local Resolve - START");
		int mappers = Integer.valueOf(context.getConfiguration().get(ATLMRHBaseMaster.RECOMMENDED_MAPPERS));
		int elements =0;
		try {
			for (TracedRule rule : execEnv.getMatches().getRules()) {
				Logger.getGlobal().log(Level.INFO, "processing rule"+rule );
				for (TraceLink link : rule.getLinks()) {
					addMapping (traceCreator, link);
					if (! execEnv.preApplySingleTrace(link)) {
						context.write(new LongWritable(randomGen.nextInt(mappers)), new Text(execEnv.getCurrentFLink().neoemfId()));
						mapTask.getTraceResource().getContents().add(mapTask.getExecutionEnv().getCurrentFLink());
						//mapTask.getExecutionEnv().getSerializableLinks().add(mapTask.getExecutionEnv().getCurrentFLink());
						elements++;
					}
				}
			}

			Logger.getGlobal().log(Level.INFO, "Local Resolve  - END");

			Logger.getGlobal().log(Level.INFO, "Cleaning up mapper - START");

			Resource resource = mapTask.getOutModel().getResource();
			resource.save(Collections.emptyMap());
			//mapTask.getTraceResource().save(Collections.emptyMap());
			// implement an ADDALL and A REMOVE for the getEcontents EList
			//			List contents = mapTask.getTraceResource().getContents();
			//			for (EObject flink : mapTask.getExecutionEnv().getSerializableLinks() ) {
			//				mapTask.getTraceResource().getContents().add(flink);
			//			}
			//			System.out.println("the number of elements to be added are "+ Boolean.toString(mapTask.getExecutionEnv().getSerializableLinks().size() == elements));
		} catch (Exception e) {
			e.printStackTrace();
		}

		Logger.getGlobal().log(Level.INFO, "Cleaning up mapper - END");
	}

	protected void addMapping(Creator traceCreator, TraceLink link) {

		String source = ((NeoEMFEObject) link.getSourceElements().get(0).getRuntimeObject()).neoemfId();
		String [] targets = resolveTargets (link.getTargetElements());
		traceCreator.addMapping(source, targets);

	}

	protected String[] resolveTargets(EList<TargetElement> targetElements) {

		String[] targets = new String[targetElements.size()];
		int index = 0;
		for (TargetElement element : targetElements) {
			targets[index++] = ((NeoEMFEObject) element.getRuntimeObject()).neoemfId();
		}
		return targets;
	}

}
