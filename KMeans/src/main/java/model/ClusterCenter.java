package model;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import io.HDFSAccessor;
import io.ParsingUtil;

public class ClusterCenter extends Vector{
	
	public ClusterCenter(double[] d) {
		super(d);
	}

	private IntWritable number;

	public IntWritable getNumber() {
		return number;
	}

	public void setNumber(IntWritable number) {
		this.number = number;
	}

	public static List<ClusterCenter> readClusterCentersFile(Configuration config, String centroidFileUri) {
		ArrayList<ClusterCenter> result = new ArrayList<>();
		
		ParsingUtil pu = new ParsingUtil(":");
		HDFSAccessor hdfs = new HDFSAccessor(config);
		hdfs.readFile(centroidFileUri, (ln,line) -> {
			StringBuffer lineBuffer = new StringBuffer(line);
			IntWritable id = new IntWritable(pu.parseInteger(lineBuffer));
			double[] vec = pu.parseDoubleArray(lineBuffer, ",");
			ClusterCenter cc = new ClusterCenter(vec);
			cc.setNumber(id);
			result.add(cc);
		});
		return result;
	}
}
