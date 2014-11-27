package fr.inria.atlanmod.atl_mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.BinaryResourceImpl;
import org.eclipse.m2m.atl.emftvm.ExecEnv;
import org.eclipse.m2m.atl.emftvm.ExecPhase;
import org.eclipse.m2m.atl.emftvm.Rule;
import org.eclipse.m2m.atl.emftvm.trace.TargetElement;
import org.eclipse.m2m.atl.emftvm.trace.TraceFactory;
import org.eclipse.m2m.atl.emftvm.trace.TraceLink;
import org.eclipse.m2m.atl.emftvm.trace.TracedRule;

public class ATLMRReducer extends Reducer<Text, BytesWritable, Text, Text> {

	private ATLMapReduceTask reduceTask = new ATLMapReduceTask();

	@Override
	protected void setup(Context context) {
		Logger.getGlobal().log(Level.INFO, "Setting up reducer - START");
		reduceTask.setup(context.getConfiguration(), false);
		reduceTask.getExecutionEnv().setExecutionPhase(ExecPhase.POST);
		Logger.getGlobal().log(Level.INFO, "Setting up reducer - END");
	}

	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

		Logger.getGlobal().log(Level.INFO, "Reduce - START");

		// TODO Parallelize this
		ExecEnv executionEnv = reduceTask.getExecutionEnv();
		ResourceSet rs = reduceTask.getRs();

		Logger.getGlobal().log(Level.INFO, "\tRegister Packages - START");
		for (Entry<String, Object> entry : reduceTask.getRs().getPackageRegistry().entrySet()) {
			rs.getPackageRegistry().put(entry.getKey(), entry.getValue());
		}
		Logger.getGlobal().log(Level.INFO, "\tRegister Packages - END");

		ByteArrayInputStream bais = null;
		Iterator<BytesWritable> links = values.iterator();
		Map<String, TracedRule> savedTracedRulesMap = new HashMap<String, TracedRule>();

		Logger.getGlobal().log(Level.INFO, "\tMerging traces - START");

		int numberOfTrace = 1;
		for (BytesWritable b; links.hasNext();) {
			Logger.getGlobal().log(Level.INFO, "\t\tProcessing trace model " + numberOfTrace++);
			b = links.next();
			bais = new ByteArrayInputStream(b.getBytes());
			Resource resource = new BinaryResourceImpl();
			rs.getResources().add(resource);
			resource.setURI(URI.createURI(""));
			resource.load(bais, Collections.emptyMap());

			for (EObject eObject : resource.getContents()) {

				TracedRule tracedRule = (TracedRule) eObject;

				TraceLink traceLink = tracedRule.getLinks().get(0);
				traceLink.getSourceElements().get(0).setRuntimeObject(traceLink.getSourceElements().get(0).getObject());

				Rule rule = executionEnv.getRulesMap().get(tracedRule.getRule());

				TracedRule savedTracedRule = savedTracedRulesMap.get(rule.getName());

				if (savedTracedRule == null) {
					savedTracedRule = TraceFactory.eINSTANCE.createTracedRule();
					savedTracedRule.setRule(tracedRule.getRule());
					savedTracedRulesMap.put(rule.getName(), savedTracedRule);
					executionEnv.getTraces().getRules().add(savedTracedRule);
				}

				savedTracedRule.getLinks().addAll(tracedRule.getLinks());

				for(TargetElement targetElement : traceLink.getTargetElements()) {
					EObject targetElementObject = targetElement.getObject();
					reduceTask.getOutModel().getResource().getContents().add(targetElementObject);
					targetElement.setRuntimeObject(targetElementObject);
				}

				rule.createDefaultMappingForTrace(traceLink);
			}

		}
		Logger.getGlobal().log(Level.INFO, "\tMerging traces - END");

		Logger.getGlobal().log(Level.INFO, "\tpostApplyAll - START");
		executionEnv.postApplyAll(reduceTask.getRs());
		Logger.getGlobal().log(Level.INFO, "\tpostApplyAll - END");

		Logger.getGlobal().log(Level.INFO, "Reduce - START");

	}

	@Override
	protected void cleanup(Reducer<Text, BytesWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		Logger.getGlobal().log(Level.INFO, "Cleaning up reducer - START");
		Resource outResource = reduceTask.getOutModel().getResource();
		outResource.save(Collections.EMPTY_MAP);
		super.cleanup(context);
		Logger.getGlobal().log(Level.INFO, "Cleaning up reducer - END");
		// TODO add resource clean up delete the intermediate models
	}

	//	private void mergeTraces(TracedRule tracedRule) throws IOException {
	//		ExecEnv executionEnv = reduceTask.getExecutionEnv();
	//		Resource outRsc = reduceTask.getOutModel().getResource();
	//
	//		TraceLinkSet traces = executionEnv.getTraces();
	//		TraceLink traceLink = tracedRule.getLinks().get(0);
	//		EObject sourceObject = traceLink.getSourceElements().get(0).getObject();
	//		traceLink.getSourceElements().get(0).setRuntimeObject(sourceObject);
	//		Rule rule = executionEnv.getRulesMap().get(tracedRule.getRule());
	//
	//		boolean notApplied = true;
	//		for (Iterator<TracedRule> iter = traces.getRules().iterator(); iter.hasNext() && notApplied;) {
	//			TracedRule tRule = iter.next();
	//			if (tRule.getRule().equals(tracedRule.getRule())) {
	//				tRule.getLinks().add(tracedRule.getLinks().get(0));
	//				notApplied = false;
	//			}
	//		}
	//
	//		if (notApplied) {
	//			traces.getRules().add(tracedRule);
	//		}
	//
	//		try {
	//			for(TargetElement te : traceLink.getTargetElements()) {
	//				EObject targetObject = te.getObject();
	//				outRsc.getContents().add(targetObject);
	//				te.setRuntimeObject(targetObject);
	//			}
	//		} catch (Exception e) {
	//			e.printStackTrace();
	//		}
	//
	//		rule.createDefaultMappingForTrace(traceLink);
	//	}

}