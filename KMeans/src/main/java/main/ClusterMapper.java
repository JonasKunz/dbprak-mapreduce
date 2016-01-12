package main;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import model.ClusterCenter;
import model.Pair;
import model.Vector;

public class ClusterMapper extends Mapper<Text, Vector, ClusterCenter, Pair<Text,Vector>>{

	public static final String CENTROIDS_FILE_CONFIG_KEY = "CENTROIDS_FILE";

	private List<ClusterCenter> getClustersCenters(Context context) {
		
		Configuration config = context.getConfiguration();
		String centroidFileUri = config.get(CENTROIDS_FILE_CONFIG_KEY);
		
		return ClusterCenter.readClusterCentersFile(config, centroidFileUri);
	}


	@Override
	protected void map(Text key, Vector value, Context context){
		//search for the closest center
		ClusterCenter closest = null;
		double closestDist = Double.MAX_VALUE;
		for(ClusterCenter cluster : getClustersCenters(context)) {
			double dist = cluster.cosineDistance(value);
			if(dist < closestDist) {
				closestDist = dist;
				closest = cluster;
			}
		}
		try {
		    	if(key != null){
		    	    context.write(closest, new Pair<>(key, value));
		    	}else{
		    	    System.out.println("Key was null");
		    	}
		} catch (Exception e) {
			RuntimeException up = new RuntimeException(e);
			throw up; 
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	
	
}
