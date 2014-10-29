package fr.inria.atlanmod.atl_mr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.xmi.XMLResource;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceImpl;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;

/* *
 * @author Amine BENELALLAM
 * 
 * In this version we are only distributing tasks
 * The number of task is not that big, thus Using IntWritable as a key 
 * 
 * TODO: specializing every Text class to a proper specification 
 * example : TransformationText, ModelText
*/



public class ATLMRMapper extends Mapper<LongWritable,Text,Text,BytesWritable> {
	
	private ATLMapReduceTask mapTask =  new ATLMapReduceTask();
	
	@Override 
	protected void  setup(Context context) throws IOException ,InterruptedException {
		mapTask.setup(context.getConfiguration());
	};
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Logger logger = ATLMapReduceTask.getLogger();
		Model inModel = mapTask.getInModel();
		ExecEnv executionEnv = mapTask.getExecutionEnv();
		String moduleName = mapTask.getModuleName();
		Record currentRecord = new Record(value);
		EObject currentObj = inModel.getResource().getEObject(currentRecord.objectFragmentUri);
		Map<String, Object> options = new HashMap<String, Object>();
		options.put(XMLResource.OPTION_ENCODING, "UTF-8"); // set encoding to utf-8
		options.put(XMLResource.OPTION_BINARY, Boolean.TRUE);
		if (executionEnv.matchSingleObject(currentObj, currentRecord.getRuleName())) {
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TraceLink currentLink = executionEnv.getCurrentMatch();
        TracedRule currentRule = currentLink.getRule();
        Resource resource = new XMIResourceImpl();
        resource.getContents().addAll(org.eclipse.emf.ecore.util.EcoreUtil.copyAll(currentRule.getLinkSet().eContents()));
        resource.save(baos, options);

		context.write(new Text(moduleName), new BytesWritable(baos.toByteArray()));
		logger.info(String.format("here is the pair key value <%s,%s>", currentRecord.getObjectFragmentUri(), currentRecord.getRuleName()));
//		td.finish();
		}
	}
	
	
		
	private static class Record {
		
		String objectFragmentUri;
		String className;
		
		public Record(Text recordValue) {
			int length = recordValue.getLength();
			int ruleStartIndex = recordValue.toString().indexOf(',');
			objectFragmentUri = recordValue.toString().substring(1, ruleStartIndex);
			className = recordValue.toString().substring(ruleStartIndex+1, length-1);
		}

		public String getObjectFragmentUri() {
			return objectFragmentUri;
		}

		public String getRuleName() {
			return className;
		}
		
	}
}