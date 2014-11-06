package fr.inria.atlanmod.atl_mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.util.BasicExtendedMetaData;
import org.eclipse.emf.ecore.util.ExtendedMetaData;
import org.eclipse.emf.ecore.xmi.XMLResource;
import org.eclipse.emf.ecore.xmi.impl.EcoreResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;
import org.eclipse.m2m.atl.emftvm.impl.resource.EMFTVMResourceFactoryImpl;

import fr.inria.atlanmod.atl_mr.utils.ATLMRUtils;


@SuppressWarnings("deprecation")
public class ATLMRMaster extends Configured implements Tool {

	public static String TRANSFORMATION = "atl.transformation";
	public static String SOURCE_METAMODEL = "atl.metamodel.source";
	public static String TARGET_METAMODEL = "atl.metamodel.target";
	public static String INPUT_MODEL = "atl.model.input";
	public static String OUTPUT_MODEL = "atl.model.output";
	
	public static final int TRANSFORMATION_ID=0;
	public static final int SOURCE_METAMODEL_ID=1;
	public static final int TARGET_METAMODEL_ID=2;
	public static final int INPUT_MODEL_ID=3;
	public static final int OUTPUT_MODEL_ID = 4;
	
	public int run(String[] args) throws Exception {
		
		ATLMRConfigEnv configurationEnv = new ATLMRConfigEnv(args[1], args[3], args[0]);
		// instantiating the Job
		
		Job job = Job.getInstance(getConf(), "ATL in MapReduce");
		job.setJarByClass(ATLMRMaster.class);
		getConf().get("dfs.name.directory");
		job.setInputFormatClass(NLineInputFormat.class);
		getConf().setInt(NLineInputFormat.LINES_PER_MAP, 5);
		getConf().set("dfs.name.directory", "file:/hadoop/hadoop-2.5.1/data/dfs");
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		//job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(ATLMRMapper.class);
		job.setReducerClass(ATLMRReducer.class);
		FileInputFormat.setInputPaths(job, configurationEnv.records());
		FileOutputFormat.setOutputPath(job, new Path("/MR_output"));
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(FileOutputFormat.getOutputPath(job), true);
		
		/*
		 * TODO Checking if a file exist
		 */
		
////		FSDataInputStream inModel = fileSystem.open(inputMModel);
////		
////		FileInputFormat.setInputPaths(job, inputRecords);
////		
////		FileOutputFormat.setOutputPath(job, new Path("/output"));
////		Job jobby= Job.getInstance(getConf());
////		jobby.setInputFormatClass(NLineInputFormat.class);
////		getConf().setInt(NLineInputFormat.LINES_PER_MAP, 5);
////		//job.setInputFormat(NLineInputFormat.class);
////		job.setInt(NLineInputFormat.LINES_PER_MAP, 5);
////		jobby.setOutputFormat(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
////		
////		jobby.setMapperClass(ATLMRMapper.class);
////		jobby.setReducerClass(ATLMRReducer.class);
////		
////		jobby.setOutputKeyClass(Text.class);
////		jobby.setOutputValueClass(IntWritable.class);
////		
//		//DistributedCache.addCacheFile(new URI((String)args[3]), job);
//		/*
//		 * TODO replace implementation with import org.apache.hadoop.mapreduce.Job
//		 * instead of JobConfiguration
//		 */
//		Path[] sharedResources = configurationEnv.formatPaths(args,getConf().get("dfs.name.directory"));
//		FileSystem fs = FileSystem.get(getConf());
//		fs.delete(FileOutputFormat.getOutputPath(job), true);
//		fs.copyFromLocalFile(new Path((String)args[0]), sharedResources[0]);
//		
//		DistributedCache.addCacheFile( new java.net.URI(sharedResources[0].toString()), getConf());	
//		DistributedCache.addCacheFile( new java.net.URI(sharedResources[1].toString()), getConf());
//		DistributedCache.addCacheFile( new java.net.URI(sharedResources[2].toString()), getConf());
//		DistributedCache.addCacheFile( new java.net.URI(sharedResources[3].toString()), getConf());
//	    DistributedCache.createSymlink(getConf());	
	//	DistributedCache.getLocalCacheFiles(getConf());
		
		job.getConfiguration().set(TRANSFORMATION, (String)args[TRANSFORMATION_ID]);
		job.getConfiguration().set(SOURCE_METAMODEL,(String)args[SOURCE_METAMODEL_ID]);
		job.getConfiguration().set(TARGET_METAMODEL,(String)args[TARGET_METAMODEL_ID]);
		job.getConfiguration().set(INPUT_MODEL,(String)args[INPUT_MODEL_ID]);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private static void registerMM(ResourceSetImpl rs, Resource mmResource) {

		EObject eObject = mmResource.getContents().get(0);
		if (eObject instanceof EPackage) {
			EPackage p = (EPackage) eObject;
			rs.getPackageRegistry().put(p.getNsURI(), p);
		}

	}
	public static void main(String[] args) throws Exception {
		ATLMRMaster driver = new ATLMRMaster();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
	
	
	public static class ATLMRConfigEnv {
		
		Resource transformationResource;
		Resource mmResource;
		Resource inputResource;
		
		public Resource getTransformationResource() {
			return transformationResource;
		}

		public void setTransformationResource(Resource transformationResource) {
			this.transformationResource = transformationResource;
		}

		public Resource getMmResource() {
			return mmResource;
		}

		public void setMmResource(Resource mmResource) {
			this.mmResource = mmResource;
		}

		public Resource getInputResource() {
			return inputResource;
		}

		public void setInputResource(Resource inputResource) {
			this.inputResource = inputResource;
		}
		
		public ATLMRConfigEnv(String args, String args2, String args3) {
			
				ResourceSetImpl rset = new ResourceSetImpl();
				final ExtendedMetaData extendedMetaData = new BasicExtendedMetaData(
						rset.getPackageRegistry());
				rset.getLoadOptions().put(XMLResource.OPTION_EXTENDED_META_DATA,
						extendedMetaData);
		
				Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put(
						"ecore", new EcoreResourceFactoryImpl());
		
				Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put(
						"xmi", new XMIResourceFactoryImpl());
				Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put(
						"emftvm", new EMFTVMResourceFactoryImpl());
		
				////////
				
				transformationResource = rset.getResource(URI
						.createURI(args3),true);
				mmResource = rset.getResource(URI
						.createURI(args),true);
			   
				registerMM(rset, mmResource );
				
				inputResource = rset.createResource(URI.createURI(args2));
				
				try {
					
					inputResource.load(null);
					mmResource.load(null);
					transformationResource.load(null);
										
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		
		public ATLMRConfigEnv(Object[] args) {
			
			/**
			 * TODO implement for [] args 
			 */
			/*
			 * TODO throw the right exception ATLMRInputException
			 * when an input path is missing
			 */
//			if(args.length != 5) {
//				System.err.println("Missing input. ATL MapReduce: <ATL transformation>, <Input MM>, <Output MM>, < Input M>, <Output directory>\nNB: The output file is deduced from the input name concatenated with ");
//				System.exit(-1);
//			}
			
		}
		Path records () throws IllegalArgumentException, IOException {
			return  new Path(ATLMRUtils.writeRecordsToFile(transformationResource, inputResource).getPath());
		}
	
		Path [] formatPaths(Object [] args, String prefix) {
			//prefix+="/";
			prefix="file:/";
			Path[] paths = new Path[4];
				for (int i=0; i<4; i++) {
					String str = (String)args[i];
					paths[i] = new Path(prefix+str);
				}
			return paths;
		}		
	}
}
