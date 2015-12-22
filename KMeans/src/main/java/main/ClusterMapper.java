package main;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import model.*;

public class ClusterMapper extends Mapper<Text, Vector, ClusterCenter, Pair<Text,Vector>>{

	private List<ClusterCenter> getClustersCenters(Context context) {
		//TODO
		return null;
		
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
			context.write(closest, new Pair<>(key, value));
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
