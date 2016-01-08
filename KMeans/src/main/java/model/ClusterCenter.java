package model;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import io.HDFSAccessor;
import io.ParsingUtil;

public class ClusterCenter extends Vector{
	
	public ClusterCenter(double[] d) {
		super(d);
	}

	private int number;

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}

	public static List<ClusterCenter> readClusterCentersFile(Configuration config, String centroidFileUri) {
		ArrayList<ClusterCenter> result = new ArrayList<>();
		
		ParsingUtil pu = new ParsingUtil(":");
		HDFSAccessor hdfs = new HDFSAccessor(config);
		hdfs.readFile(centroidFileUri, (ln,line) -> {
			StringBuffer lineBuffer = new StringBuffer(line);
			int id = pu.parseInteger(lineBuffer);
			double[] vec = pu.parseDoubleArray(lineBuffer, ",");
			ClusterCenter cc = new ClusterCenter(vec);
			cc.setNumber(id);
			result.add(cc);
		});
		return result;
	}
}
