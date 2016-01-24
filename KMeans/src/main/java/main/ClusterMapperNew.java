package main;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import io.ParsingUtil;
import model.ClusterCenter;
import model.ClusterCenterWritable;
import model.Pair;
import model.Vector;
import model.VectorWritable;

public class ClusterMapperNew extends Mapper<LongWritable, Text, ClusterCenterWritable, VectorWritable>{

	private static final String ELEMENT_SEPARATOR = ":";
	private static final String ARRAY_SEPARATOR = ",";
	
	public static final String CENTROIDS_FILE_CONFIG_KEY = "CENTROIDS_FILE";
	private ParsingUtil pu;
	

	private List<ClusterCenter> getClustersCenters(Context context) {
		
		Configuration config = context.getConfiguration();
		String centroidFileUri = config.get(CENTROIDS_FILE_CONFIG_KEY);
		return ClusterCenter.readClusterCentersFile(config, centroidFileUri);
	}


	@Override
	protected void map(LongWritable key, Text value, Context context){
		//file format is word:vector
		String originalLine = value.toString();
		try {
			StringBuffer line = new StringBuffer(originalLine);
			String word = pu.parseString(line); //parse the word
			double[] vectorValues = pu.parseDoubleArray(line, ARRAY_SEPARATOR);
			Vector vector = new Vector(vectorValues);
			
			//search for the closest center
			ClusterCenter closest = null;
			double closestDist = Double.MAX_VALUE;
			for(ClusterCenter cluster : getClustersCenters(context)) {
				//early out check: the vector might have been parsed wit ha wrong idmension
				if(cluster.getDimension() != vector.getDimension()) {
					return;
				}
				double dist = cluster.cosineDistance(vector);
				if(dist < closestDist) {
					closestDist = dist;
					closest = cluster;
				}
			}
	    	if(key != null){
	    	    context.write(new ClusterCenterWritable(closest), new VectorWritable(vector));
	    	}else{
	    	    System.out.println("Key was null");
	    	}
		} catch(Exception e) {
			System.out.println("Skipping faulting line (" +e.getMessage()+"): "+originalLine);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		pu = new ParsingUtil(ELEMENT_SEPARATOR);
		super.setup(context);
	}

	
	
}
