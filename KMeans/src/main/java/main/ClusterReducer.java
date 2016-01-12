package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import model.ClusterCenter;
import model.ClusterCenterWritable;
import model.Pair;
import model.Vector;
import model.VectorWritable;

public class ClusterReducer extends Reducer<ClusterCenterWritable, VectorWritable, IntWritable, VectorWritable>{

	@Override
	protected void reduce(ClusterCenterWritable cluster, Iterable<VectorWritable> words, Context context) throws IOException, InterruptedException {
		Vector sum = null;
		int count = 0;
		for(VectorWritable vec : words) {
			Vector normalized = new Vector(vec.getVector().getData());
			normalized.normalize();
			count++;
			if(sum == null) {
				sum = normalized;
			} else {
				sum.add(normalized);
			}
		}
		if(sum == null) {
			context.write(cluster.getCenter().getNumber(), new VectorWritable(cluster.getCenter()));
		} else {
			sum.mapMultiplyToSelf(1.0 / count);
			context.write(cluster.getCenter().getNumber(), new VectorWritable(sum));
		}
		
	}
	
	

}
