package main;

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
		for (ClusterCenter center : centers) {
			Iterator<String> it = hdfs.readFile(inputPath);
			hdfs.clearAndWriteFile(outputPath + center.getNumber().get() + "/words", (lnr) -> {
				String lineToWrite = null;
				while (lineToWrite == null && it.hasNext()) {
					String originalLine = it.next();
					try {
						StringBuffer line = new StringBuffer(originalLine);
						String word = pu.parseString(line); // parse the word
						double[] vectorValues = pu.parseDoubleArray(line, ARRAY_SEPARATOR);
						Vector vector = new Vector(vectorValues);
						
						ClusterCenter closest = getClosestCenter(centers, vector);
						if(closest == null) {
							continue;
						}
						if (closest.equals(center)) {
							lineToWrite = originalLine;
						}
					} catch(Exception e) {
					}
				}
				return lineToWrite;
			});
		}
		Iterator<String> it = hdfs.readFile(inputPath);
		hdfs.clearAndWriteFile(mergedOutputFile, (lnr) -> {
			while(it.hasNext()) {
				String originalLine = it.next();
				try {
					StringBuffer line = new StringBuffer(originalLine);
					String word = pu.parseString(line); // parse the word
					double[] vectorValues = pu.parseDoubleArray(line, ARRAY_SEPARATOR);
					Vector vector = new Vector(vectorValues);
					ClusterCenter closest = getClosestCenter(centers, vector);
					if(closest == null) {
						continue;
					}
					return closest.getNumber().get()+ELEMENT_SEPARATOR+originalLine;
				} catch(Exception e) {
				}
			}
			return null;
		});

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
