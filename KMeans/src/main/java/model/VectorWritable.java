package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import io.ParsingUtil;

public class VectorWritable implements Writable{

	private Vector vector;
	
	public VectorWritable() {
		super();
	}
	
	public VectorWritable(Vector vector) {
		super();
		this.vector = vector;
	}

	public Vector getVector() {
		return vector;
	}

	public void setVector(Vector vector) {
		this.vector = vector;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringBuffer data = new StringBuffer();
		for(double d : vector.getData()) {
			if(data.length() != 0) {
				data.append(',');
			}
			data.append(Double.toString(d));
		}
		out.writeUTF(data.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ParsingUtil pu = new ParsingUtil(":");
		StringBuffer data = new StringBuffer(in.readUTF());
		vector = new Vector(pu.parseDoubleArray(data, ","));
		
		
	}

}
