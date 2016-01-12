package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import io.ParsingUtil;

public class ClusterCenterWritable implements WritableComparable<ClusterCenterWritable>{

	private ClusterCenter center;
	
	
	

	public ClusterCenterWritable() {
		super();
	}

	public ClusterCenterWritable(ClusterCenter center) {
		super();
		this.center = center;
	}

	public ClusterCenter getCenter() {
		return center;
	}

	public void setCenter(ClusterCenter center) {
		this.center = center;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringBuffer data = new StringBuffer();
		data.append(Integer.toString(center.getNumber().get()));
		data.append(':');
		boolean first = true;
		for(double d : center.getData()) {
			if(!first) {
				data.append(',');
				first = false;
			}
			data.append(Double.toString(d));
		}
		out.writeUTF(data.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ParsingUtil pu = new ParsingUtil(":");
		StringBuffer data = new StringBuffer(in.readUTF());
		
		int id = pu.parseInteger(data);
		center = new ClusterCenter(pu.parseDoubleArray(data, ","));
		center.setNumber(new IntWritable(id));
		
		
	}

	@Override
	public int compareTo(ClusterCenterWritable o) {
		return center.getNumber().compareTo(o.center.getNumber());
	}

}
