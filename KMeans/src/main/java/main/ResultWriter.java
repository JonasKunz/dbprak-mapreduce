package main;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import io.HDFSAccessor;
import io.ParsingUtil;
import io.WriterUtil;
import model.ClusterCenter;
import model.Vector;
import util.Writable;

public class ResultWriter {

	private String inputPath;

	private static final String ELEMENT_SEPARATOR = ":";
	private static final String ARRAY_SEPARATOR = ",";

	private HDFSAccessor hdfs;

	public ResultWriter(HDFSAccessor hdfs, String inputPath) {
		super();
		this.inputPath = inputPath;
		this.hdfs = hdfs;
	}

	public void assignCenters(List<ClusterCenter> centers, String outputPath, String mergedOutputFile) {
		ParsingUtil pu = new ParsingUtil(ELEMENT_SEPARATOR);
		
		BufferedWriter allWordsFile = hdfs.clearAndWriteFile(mergedOutputFile);
		final Map<Integer, BufferedWriter> partitionFiles = new HashMap<>();
		final Set<BufferedWriter> usedWriters = new HashSet<>();
		
		for (ClusterCenter center : centers) {
			BufferedWriter bw = hdfs.clearAndWriteFile(outputPath + center.getNumber().get() + "/words");
			partitionFiles.put(center.getNumber().get(), bw);
		}
		
		hdfs.readFile(inputPath ,(ln,originalLine) -> {
			try {
				StringBuffer line = new StringBuffer(originalLine);
				String word = pu.parseString(line); // parse the word
				double[] vectorValues = pu.parseDoubleArray(line, ARRAY_SEPARATOR);
				Vector vector = new Vector(vectorValues);
				ClusterCenter closest = getClosestCenter(centers, vector);
				if(closest != null) {
					if(!usedWriters.contains(allWordsFile)) {
						usedWriters.add(allWordsFile);
					} else {
						allWordsFile.newLine();
					}
					allWordsFile.write( closest.getNumber().get()+ELEMENT_SEPARATOR+originalLine);
					
					BufferedWriter partitionWriter = partitionFiles.get(closest.getNumber().get());
					if(!usedWriters.contains(partitionWriter)) {
						usedWriters.add(partitionWriter);
					} else {
						partitionWriter.newLine();
					}
					partitionWriter.write(originalLine);
					
				}
			} catch(Exception e) {
				//System.out.println("Skipping faulting line (" +e.getMessage()+"): "+originalLine);
			}
		});
		
		try {
			allWordsFile.close();
			for(BufferedWriter bw : partitionFiles.values()) {
				bw.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private ClusterCenter getClosestCenter(List<ClusterCenter> centers, Vector vector) {
		ClusterCenter closest = null;
		double closestDist = Double.MAX_VALUE;
		for (ClusterCenter cluster : centers) {
			//fix for worngly parsed vector
			if(vector.getDimension() != cluster.getDimension()) {
				return null;
			}
			double dist = cluster.cosineDistance(vector);
			if (dist < closestDist) {
				closestDist = dist;
				closest = cluster;
			}
		}
		return closest;
	}
}
