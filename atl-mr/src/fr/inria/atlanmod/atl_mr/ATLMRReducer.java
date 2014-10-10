package fr.inria.atlanmod.atl_mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public  class ATLMRReducer extends Reducer<Text,Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable values,
			Context context)
			throws IOException, InterruptedException {
		
//		int sum = 0;
//		for (IntWritable value:values) {
//			sum += value.get();
//		}
//		context.write(key, new IntWritable(sum));
		
	}
}