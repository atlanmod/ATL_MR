package fr.inria.atlanmod.atl_mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.eclipse.emf.ecore.EObject;

public class EMFTVMInputFormat extends SequenceFileInputFormat<Text, EObject> {
	
	public RecordReader<Text, EObject> getRecordReader ( InputSplit input, JobConf job, Reporter reporter)
		      throws IOException {
		reporter.setStatus(input.toString());
		return new EMFTVMRecordReader(job, (FileSplit)input);
		
	}

}
