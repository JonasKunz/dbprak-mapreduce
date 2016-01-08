package main;

import java.io.IOException;

import io.WordVectorInputFormat;
import model.ClusterCenter;
import model.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans {

    private final String JOB_NAME = "KMeans";

    public void kmeans(String[] args) {
	String input = args[0];
	String output = args[1];

	Configuration config = new Configuration();
	Path in = new Path("");
	Path center = new Path("");
	Path out = new Path("");
	Job job;
	try {
	    job = new Job(config);
	    job.setJobName(JOB_NAME);
	    job.setMapperClass(ClusterMapper.class);
	    job.setReducerClass(ClusterReducer.class);
	    job.setInputFormatClass(WordVectorInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setOutputKeyClass(ClusterCenter.class);
	    job.setOutputValueClass(Vector.class);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	int iteration = 0;
	boolean isdone = false;
	while (isdone == false) {
	}

    }

}
