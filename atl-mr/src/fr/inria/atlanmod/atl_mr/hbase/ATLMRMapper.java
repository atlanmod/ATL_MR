package fr.inria.atlanmod.atl_mr.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.ExecPhase;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.trace.TargetElement;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;

import fr.inria.atlanmod.atl_mr.utils.HbaseTraceCreator;
import fr.inria.atlanmod.atl_mr.utils.Tracer;
import fr.inria.atlanmod.atl_mr.utils.Tracer.Creator;
import fr.inria.atlanmod.kyanos.core.KyanosEObject;

/* *
 * @author Amine BENELALLAM
 *
 * In this version we are only distributing tasks
 * The number of task is not that big, thus Using IntWritable as a key
 *
 */

public class ATLMRMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

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
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Logger.getGlobal().log(Level.FINEST, "Mapping - START - " + value.toString());

		try {
			Model inModel = mapTask.getInModel();
			ExecEnv executionEnv = mapTask.getExecutionEnv();
			Record currentRecord = new Record(value);
			EObject currentObj = inModel.getResource().getEObject(currentRecord.objectFragmentUri);
			System.out.println("****"+currentObj+"****");
			Logger.getGlobal().log(Level.FINEST, "\tmatchSingleObject - START");
			//			if (executionEnv.preApplySingleObject (currentObj, currentRecord.className)) {
			//
			//				TraceLink currentLink = executionEnv.getCurrentMatch();
			//				TracedRule currentRule = currentLink.getRule();
			//				FLink flink = mapTask.getExecutionEnv().flattenLink();
			//				context.write(new LongWritable(1), new Text(flink.kyanosId()));
			//				//tracesResource.getContents().add(ATLMRUtils.copyRule(currentRule, currentLink));
			//			}
			System.out.println(executionEnv.matchSingleObject(currentObj, currentRecord.className));
			Logger.getGlobal().log(Level.FINEST, "\tmatchSingleObject - END");

			Logger.getGlobal().log(Level.FINEST, "Mapping - END");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		ExecEnv execEnv = mapTask.getExecutionEnv();
		Tracer.Creator traceCreator = (Creator) mapTask.getTracer();
		Logger.getGlobal().log(Level.INFO, "Cleaning up mapper - START");
		try {
			for (TracedRule rule : execEnv.getMatches().getRules()) {
				for (TraceLink link : rule.getLinks()) {
					addMapping (traceCreator, link);
					if (execEnv.preApplySingleTrace(link)) {
						context.write(new LongWritable(1), new Text(execEnv.getCurrentFLink().kyanosId()));
					}
				}
			}

			Resource resource = mapTask.getOutModel().getResource();
			resource.save(Collections.emptyMap());
			mapTask.getTraceResource().save(Collections.emptyMap());
			mapTask.getTraceResource().getContents().addAll(mapTask.getExecutionEnv().getSerializableLinks());
		} catch (Exception e) {
			e.printStackTrace();
		}
		//		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		//		tracesResource.save(baos, Collections.emptyMap());
		//		context.write(new Text(mapTask.getModuleName()), new BytesWritable(baos.toByteArray()));
		Logger.getGlobal().log(Level.INFO, "Cleaning up mapper - END");
	}

	private void addMapping(Creator traceCreator, TraceLink link) {
		String source = ((KyanosEObject) link.getSourceElements().get(0).getRuntimeObject()).kyanosId();
		String [] targets = resolveTargets (link.getTargetElements());
		traceCreator.addMapping(source, targets);

	}

	private String[] resolveTargets(EList<TargetElement> targetElements) {
		String[] targets = new String[targetElements.size()];
		int index = 0;
		for (TargetElement element : targetElements) {
			//ArrayUtils.add(targets, ((KyanosEObject) element.getRuntimeObject()).kyanosId());
			targets[index++] = ((KyanosEObject) element.getRuntimeObject()).kyanosId();
		}
		return targets;
	}

	private static class Record {

		String objectFragmentUri;
		String className;

		public Record(Text recordValue) {
			int length = recordValue.getLength();
			int ruleStartIndex = recordValue.toString().indexOf(',');
			objectFragmentUri = recordValue.toString().substring(1, ruleStartIndex);
			className = recordValue.toString().substring(ruleStartIndex + 1, length - 1);
		}
	}// end Record

}// end