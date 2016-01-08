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
import model.Vector;
import util.Writable;

public class Initializer {

	private String inputPath;	
	
	private static final String ELEMENT_SEPARATOR = ":";
	private static final String ARRAY_SEPARATOR = ",";
	
	private HDFSAccessor hdfs;
	
	
	
	
	
	public Initializer(HDFSAccessor hdfs , String inputPath) {
		super();
		this.inputPath = inputPath;
		this.hdfs = hdfs;
	}


	public void computeCenters(int n, int k, long seed, String outputPath) {
		List<Vector> workingSet = getRandomSubset(n, seed);
		List<Vector> clusterCenters = new LinkedList<>();
		
		//take the first element from the working set as center
		clusterCenters.add(workingSet.remove(0));
		//initialize the distances
		Map<Vector, Double> minimumSeedDistance = new HashMap<>();
		for(Vector vec : workingSet) {
			minimumSeedDistance.put(vec, vec.cosineDistance(clusterCenters.get(0)));
		}
		//add the one with the biggest minimum distance to the set of centers
		while (clusterCenters.size() < k) {
			//scan for the maximum distance
			Entry<Vector,Double> maximum = null;
			for(Entry<Vector,Double> ent : minimumSeedDistance.entrySet()) {
				if(maximum == null || ent.getValue() > maximum.getValue()) {
					maximum = ent;
				}
			}
			//add it to the cluster centers;
			Vector newCenter = maximum.getKey();
			clusterCenters.add(newCenter);
			minimumSeedDistance.remove(newCenter);
			//update the distance map
			for(Vector vec : minimumSeedDistance.keySet()) {
				double distance = minimumSeedDistance.get(vec);
				distance = Math.min(distance, vec.cosineDistance(newCenter));
				minimumSeedDistance.put(vec, distance);
			}
		}
		writeClusterCenters(clusterCenters, outputPath);
	}
	
	
	private void writeClusterCenters(List<Vector> clusterCenters, String outputPath) {
		Iterator<Vector> vit = clusterCenters.iterator();
		hdfs.clearAndWriteFile(outputPath, (ln) -> {
			if(vit.hasNext()) {
				WriterUtil wr = new WriterUtil(ELEMENT_SEPARATOR);
				wr.writeLong(ln);
				wr.writeDoubleArray(vit.next().getData(), ARRAY_SEPARATOR);
				return wr.toString();
			} else {
				return null; //EOF
			}
		});
	}


	List<Vector> getRandomSubset(int n, long seed) {
		Random rnd = new Random(seed);
		
		//step one read the file once to get the linecount
		Writable<Long> linecountW = new Writable<>(0L);
		hdfs.readFile(inputPath, (ln,l) -> linecountW.set(linecountW.get() + 1)); 
		long lineCount = linecountW.get();
		
		//step to: create a random set of size N
		Set<Long> linesToFetch = new HashSet<>();
		while(linesToFetch.size() < n) {
			long index = (long) (lineCount * rnd.nextDouble());
			linesToFetch.add(index);
		}
		
		//step three - read the actual values
		ParsingUtil pu = new ParsingUtil(ELEMENT_SEPARATOR);
		List<Vector> result = new LinkedList<>();
		hdfs.readFile(inputPath, (ln,l) -> {
			if(linesToFetch.contains(ln)) {
				StringBuffer line = new StringBuffer(l);
				String word = pu.parseString(line);
				double[] vector = pu.parseDoubleArray(line, ARRAY_SEPARATOR);
				result.add(new Vector(vector));
			}
		}); 
		return result;
		
	}
	
}
