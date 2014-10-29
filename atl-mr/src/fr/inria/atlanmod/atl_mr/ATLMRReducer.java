package fr.inria.atlanmod.atl_mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.BinaryResourceImpl;
import org.eclipse.emf.ecore.xmi.XMLResource;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.Model;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;
import fr.inria.atlanmod.atl_mr.utils.ATLMRUtils;



public  class ATLMRReducer extends Reducer<Text,BytesWritable, Text, Text> {
		
	private ATLMapReduceTask reduceTask = new ATLMapReduceTask();
	
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,
			Context context)
			throws IOException, InterruptedException {		
		// Parallelize this 
			ExecEnv executionEnv = reduceTask.getExecutionEnv();
			ResourceSet rs = reduceTask.getRs();
			Logger logger = ATLMapReduceTask.getLogger();
			Model outModel = reduceTask.getOutModel();
				Map<String, Object> options = new HashMap<String, Object>();
				options.put(XMLResource.OPTION_BINARY, Boolean.TRUE);
				ByteArrayInputStream bais = null;
			    	Iterator<BytesWritable> links = values.iterator();
			    	for (BytesWritable b; links.hasNext(); ) {
			    		b= links.next();
						bais = new ByteArrayInputStream(b.getBytes());
						Resource resource = new BinaryResourceImpl();
						rs.getResources().add(resource);
						resource.load(bais, Collections.emptyMap());
						ATLMRUtils.mergeTraces (executionEnv, (TracedRule)resource.getContents().get(0),rs);					
			    }
			outModel.getResource().save(System.out, Collections.emptyMap());
			executionEnv.applyAll();
			outModel.getResource().save(System.out, Collections.emptyMap());
			logger.log(Level.INFO, String.format("enter the reduce for key <%s>", key.toString()));		
	}
	
	@Override 
	protected void setup(Context context) {
		reduceTask.setup(context.getConfiguration());
	}

}