package fr.inria.atlanmod.atl_mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.eclipse.m2m.atl.emftvm.Rule;

public class ATLProcessUnit implements Writable {

	private Rule myRule;
	static int ruleRank=0;
	
	public void readFields(DataInput arg0) throws IOException {

	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

}
