package main;

import java.util.List;

import io.HDFSAccessor;
import io.WordVectorInputFormat;
import model.ClusterCenter;
import model.ClusterCenterWritable;
import model.Vector;
import model.VectorWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans {

	private final String JOB_NAME = "KMeans";
	private final String input = "/usr/local/hadoop/dbprak/public/example.txt";
	private final String outputPath = "/usr/local/hadoop/dbprak/group2/output1";

	public void kmeans(String[] args) throws Exception {

		int nValue = Integer.parseInt(args[0]);
		int kValue = Integer.parseInt(args[1]);
		int rndSeed = Integer.parseInt(args[2]);
		double threshold = Double.parseDouble(args[3]);

		String currentInput = outputPath + "/tempA";
		String currentOutput = outputPath + "/tempB";

		HDFSAccessor hdfs = new HDFSAccessor(new Configuration());

		Initializer init = new Initializer(hdfs, input);
		init.computeCenters(nValue, kValue, rndSeed, currentInput);
		List<ClusterCenter> centers = ClusterCenter.readClusterCentersFile(new Configuration(), currentInput);

		int iteration = 0;
		boolean isdone = false;
		while (isdone == false) {

			Configuration config = new Configuration();
			System.out.println("Iteration" + iteration);
			
			config.set("mapreduce.output.textoutputformat.separator", ":");
			config.set(ClusterMapper.CENTROIDS_FILE_CONFIG_KEY, currentInput);
			
			Job job;
			
			job = new Job(config);
			job.setJobName(JOB_NAME);
			job.setMapperClass(ClusterMapperNew.class);
			job.setReducerClass(ClusterReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(VectorWritable.class);
			job.setMapOutputKeyClass(ClusterCenterWritable.class);
			job.setMapOutputValueClass(VectorWritable.class);
			
			FileInputFormat.setInputPaths(job, new Path(input));	
			FileOutputFormat.setOutputPath(job, new Path(currentOutput));
			System.out.println("File Output set");

			job.waitForCompletion(true);
			System.out.println("Job complete");
			String filename = currentOutput + "/part-r-00000";
//			
			List<ClusterCenter> newCenters = ClusterCenter.readClusterCentersFile(config,filename);
			System.out.println("Check distance");

			double maxDistance = -Double.MAX_VALUE;
			for(ClusterCenter oldCC : centers) {
				for(ClusterCenter newCC : newCenters) {
					if(oldCC.getNumber().equals(newCC.getNumber())) {
						double distance = oldCC.cosineDistance(newCC);
						maxDistance = Math.max(maxDistance, distance);
					}
				}
			}
			System.out.println("Center movement of iteration no. " + iteration +": "+maxDistance);
			iteration++;
			System.out.println(maxDistance <= threshold);
			isdone = maxDistance <= threshold;	
			if(FileSystem.get(config).exists(new Path(currentInput))){
			    FileSystem.get(config).delete(new Path(currentInput),true);
			    FileUtil.copy( FileSystem.get(config), new Path(filename),  FileSystem.get(config), new Path(currentInput), true, true, config);
			}
			centers = newCenters;
			System.out.println("Delete Outputfile");
			if(FileSystem.get(config).exists(new Path(currentOutput))){
			    FileSystem.get(config).delete(new Path(currentOutput),true);
			}
		}
		
		ResultWriter rw = new ResultWriter(hdfs,input);
		rw.assignCenters(centers, outputPath + "/partitionId=");
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		FileUtil.copy( fs, new Path(currentInput),  fs, new Path(outputPath + "/clusterCenters"), true, true, config);
		fs.delete(new Path(currentInput), true);
	}

}
