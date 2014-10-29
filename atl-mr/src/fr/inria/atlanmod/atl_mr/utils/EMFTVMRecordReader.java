package fr.inria.atlanmod.atl_mr.utils;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceImpl;

import com.sun.jersey.server.impl.component.ResourceFactory;

public class EMFTVMRecordReader extends RecordReader<Text, EObject> {
	
	Resource emftvmResource;
 
	public EMFTVMRecordReader(JobConf job, FileSplit input) {
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap()
			.put("emftvm", new EMFTVMResourceFactoryImpl());
		ResourceSet resourceSet = new ResourceSetImpl();
		emftvmResource = resourceSet.getResource(URI.createURI(input.getPath().toString()), true);
	}

	@Override
	public void close() throws IOException {
		emftvmResource.unload();

	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public EObject getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

}
