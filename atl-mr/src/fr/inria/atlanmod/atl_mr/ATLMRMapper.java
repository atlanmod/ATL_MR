package fr.inria.atlanmod.atl_mr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.impl.BinaryResourceImpl;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.ExecPhase;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;

import fr.inria.atlanmod.atl_mr.utils.ATLMRUtils;

/* *
 * @author Amine BENELALLAM
 *
 * In this version we are only distributing tasks
 * The number of task is not that big, thus Using IntWritable as a key
 *
 */

public class ATLMRMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {

	private ATLMapReduceTask mapTask = new ATLMapReduceTask();
	private BinaryResourceImpl tracesResource = new BinaryResourceImpl();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Logger.getGlobal().log(Level.INFO, "Setting up mapper - START");
		mapTask.setup(context.getConfiguration(), true);
		mapTask.getExecutionEnv().setExecutionPhase(ExecPhase.PRE);
		Logger.getGlobal().log(Level.INFO, "Setting up mapper - END");
	};

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Logger.getGlobal().log(Level.FINEST, "Mapping - START - " + value.toString());

		Model inModel = mapTask.getInModel();
		ExecEnv executionEnv = mapTask.getExecutionEnv();
		Record currentRecord = new Record(value);
		EObject currentObj = inModel.getResource().getEObject(currentRecord.objectFragmentUri);

		Logger.getGlobal().log(Level.FINEST, "\tpreApplySingleObject - START");
		if (executionEnv.preApplySingleObject (currentObj, currentRecord.className)) {

			TraceLink currentLink = executionEnv.getCurrentMatch();
			TracedRule currentRule = currentLink.getRule();

			tracesResource.getContents().add(ATLMRUtils.copyRule(currentRule, currentLink));
		}
		Logger.getGlobal().log(Level.FINEST, "\tpreApplySingleObject - END");

		Logger.getGlobal().log(Level.FINEST, "Mapping - END");
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Logger.getGlobal().log(Level.INFO, "Cleaning up mapper - START");
		Resource resource = mapTask.getOutModel().getResource();
		resource.save(Collections.emptyMap());

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		tracesResource.save(baos, Collections.emptyMap());
		context.write(new Text(mapTask.getModuleName()), new BytesWritable(baos.toByteArray()));
		Logger.getGlobal().log(Level.INFO, "Cleaning up mapper - END");
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