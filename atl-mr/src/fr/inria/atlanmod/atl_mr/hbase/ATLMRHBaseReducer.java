package fr.inria.atlanmod.atl_mr.hbase;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.m2m.atl.emftvm.ExecPhase;
import org.eclipse.m2m.atl.emftvm.ftrace.FLink;
import org.eclipse.m2m.atl.emftvm.ftrace.FTraceProperty;

import fr.inria.atlanmod.atl_mr.utils.HbaseTraceResolver;
import fr.inria.atlanmod.atl_mr.utils.Tracer.Resolver;
import fr.inria.atlanmod.kyanos.core.KyanosEObject;

public class ATLMRHBaseReducer extends Reducer<LongWritable,Text, Text, Text> {

	/**
	 *
	 */
	private ATLMapReduceTask reduceTask = new ATLMapReduceTask();

	/*
	 *
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void setup(Context context) {
		Logger.getGlobal().log(Level.INFO, "Setting up reducer - START");
		reduceTask.setup(context.getConfiguration(), false);
		reduceTask.getExecutionEnv().setExecutionPhase(ExecPhase.POST);
		reduceTask.setTracer(new HbaseTraceResolver(reduceTask.getTraceResource().getURI(),
				reduceTask.getOutModel().getResource()));
		Logger.getGlobal().log(Level.INFO, "Setting up reducer - END");
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Logger.getGlobal().log(Level.INFO, "Reduce - START");

		Resource traceResource = reduceTask.getTraceResource();
		//		ExecEnv executionEnv = reduceTask.getExecutionEnv();

		Logger.getGlobal().log(Level.INFO, "\t GLobal Resolve - START");
		for (Text value : values) {
			resolveLink((FLink)traceResource.getEObject(value.toString()));
		}
		Logger.getGlobal().log(Level.INFO, "\t GLobal Resolve - END");

		Logger.getGlobal().log(Level.INFO, "Reduce - END");
	}
	/**
	 * resolves FlatLinks
	 * @param eObject
	 */
	private void resolveLink(FLink eObject) {
		assert eObject instanceof FLink;
		Resource kyanosResource = reduceTask.getOutModel().getResource();
		KyanosEObject keObject = null;
		for (FTraceProperty prop : eObject.getProperties()) {
			keObject = (KyanosEObject)kyanosResource
					.getEObject(prop.getResolvedFor());

			resolveBinding (keObject, prop, (Resolver)this.reduceTask.getTracer());
		}

	}
	/**
	 * resolves Flat properties
	 * @param keObject
	 * @param prop
	 * @param tracer
	 */
	@SuppressWarnings("unchecked")
	private void resolveBinding(KyanosEObject keObject, FTraceProperty prop, Resolver tracer) {

		EStructuralFeature ft = keObject.eClass().getEStructuralFeature(prop.getPropertyName());

		assert ft instanceof EReference;

		if (ft.isMany()) {
			EList<KyanosEObject> resolvings =  new BasicEList<KyanosEObject>();
			for (String kyanosId : prop.getResolvings()) {
				resolvings.add((KyanosEObject) tracer.resolve(kyanosId));
			}
			((List<KyanosEObject>) keObject.eGet(ft)).addAll(resolvings);
		} else {
			keObject.eSet(ft, tracer.resolve(prop.getResolvings().get(0)));
		}

	}

	@Override
	protected void cleanup(Reducer<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// cleaning up reducer
		Logger.getGlobal().log(Level.INFO, "Cleaning up reducer - START");
		super.cleanup(context);
		Logger.getGlobal().log(Level.INFO, "Cleaning up reducer - END");
	}
}