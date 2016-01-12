package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import model.ClusterCenter;
import model.Pair;
import model.Vector;

public class ClusterReducer extends Reducer<ClusterCenter, Pair<Text, Vector>, IntWritable, Vector>{

	@Override
	protected void reduce(ClusterCenter cluster, Iterable<Pair<Text, Vector>> words, Context context) throws IOException, InterruptedException {
		Vector sum = null;
		int count = 0;
		for(Pair<Text, Vector> word : words) {
			Vector normalized = new Vector(word.getSecond());
			normalized.normalize();
			count++;
			if(sum == null) {
				sum = normalized;
			} else {
				sum.add(normalized);
			}
		}
		if(sum == null) {
			context.write(cluster.getNumber(), cluster);
		} else {
			sum.mapMultiplyToSelf(1.0 / count);
			context.write(cluster.getNumber(), sum);
		}
		
	}
	
	

}
