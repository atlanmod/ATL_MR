package fr.inria.atlanmod.atl_mr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.xmi.XMLResource;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceImpl;
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

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mapTask.setup(context.getConfiguration(), true);
		mapTask.getExecutionEnv().setExecutionPhase(ExecPhase.PRE);
	};
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		try {
		Model inModel = mapTask.getInModel();
		ExecEnv executionEnv = mapTask.getExecutionEnv();
		String moduleName = mapTask.getModuleName();
		Record currentRecord = new Record(value);
		EObject currentObj = inModel.getResource().getEObject(currentRecord.objectFragmentUri);

		
		Map<String, Object> options = new HashMap<String, Object>();
		options.put(XMLResource.OPTION_ENCODING, "UTF-8"); 
		options.put(XMLResource.OPTION_BINARY, Boolean.TRUE);
		if (executionEnv.preApplySingleObject (currentObj, currentRecord.className)) {
			
			TraceLink currentLink = executionEnv.getCurrentMatch();
			TracedRule currentRule = currentLink.getRule();
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Resource resource = new XMIResourceImpl();
			resource.getContents().add(ATLMRUtils.copyRule(currentRule, currentLink));
			resource.save(baos, options);
			
			context.write(new Text(moduleName), new BytesWritable(baos.toByteArray()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
			Resource resource = mapTask.getOutModel().getResource();
			resource.save(Collections.emptyMap());
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