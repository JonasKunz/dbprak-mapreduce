import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KMeans {

	private static final String INPUT_FILE = "example.txt";
	private static final String OUTPUT_FODLER = "results";

	private static Stream<Word> getInputData() {
		try {
			return Files.lines(Paths.get(INPUT_FILE))
					.parallel()
					.map(Word::parse)
					.filter(Optional::isPresent)
					.map(Optional::get);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	

	public static void main(String[] args) throws IOException {
		
		long start = System.currentTimeMillis();
		System.out.println("initializing..");
		Map<Integer, Vector> currentCenters = getInitCenters(10000, 1000, 1);
		System.out.println("finished after " +(System.currentTimeMillis() - start) +" ms.");
		
		Vector identity = new Vector(new double[currentCenters.values().stream().findAny().get().getDimension()]);
		
		//actual kmeans Java 8 stream implementation
		double movement = 2.0;
		while(movement > 0.05) {
			start = System.currentTimeMillis();
			final Map<Integer, Vector> thisIterationCenters = currentCenters;
			Map<Integer, Vector> newCenters =  getInputData().map(Word::getVector).collect(
					Collectors.mapping(
					Vector::getNormalized, 
					Collectors.groupingByConcurrent(
							(v) -> getNearestCluster(v,thisIterationCenters), //group by key
							Collectors.reducing(identity,Vector::add))) //add up all vectors corresponding to one center
					).entrySet().stream()
					//perform normalization of newly found centers
					.collect(Collectors.toMap(Entry::getKey, (e) -> e.getValue().getNormalized()));
			
			
			movement = newCenters.entrySet().stream()
					.mapToDouble((e) -> thisIterationCenters.get(e.getKey()).cosineDistance(e.getValue()))
					.max().
					getAsDouble();
			System.out.println("center movement :" + movement);
			System.out.println("Iteration finished after " +(System.currentTimeMillis() - start) +" ms.");
			/*
			Map<Integer, Integer> clusterSizes =  getInputData().map(Word::getVector).collect(
					Collectors.mapping(
					Vector::getNormalized, 
					Collectors.groupingByConcurrent((v) -> getNearestCluster(v,thisIterationCenters), Collectors.reducing(1, (v) -> 1, (a,b) -> a+b))
					));
			System.out.println("Cluster sizes: "+ clusterSizes);
			*/
			currentCenters = newCenters;
		}
		final Map<Integer, Vector> finalCenters = currentCenters;
		
		System.out.println("Writing results...");
		if(Files.exists(Paths.get(OUTPUT_FODLER))) {
			removeRecursive(Paths.get(OUTPUT_FODLER));
		}
		Files.createDirectory(Paths.get(OUTPUT_FODLER));
		
		//cluster centers
		Iterable<String> data = () -> 
				finalCenters.entrySet()
				.stream()
				.map((e) -> e.getKey()+Constants.ELEMENT_SEPARATOR+e.getValue())
				.iterator();
		Files.write(Paths.get(OUTPUT_FODLER+"/clusterCenters"), data); 
		
		final BufferedWriter allWordsFile = Files.newBufferedWriter(Paths.get(OUTPUT_FODLER+"/allWords"));
		final Map<Integer,BufferedWriter> outputFiles = new HashMap<>();
		for(int centerId : finalCenters.keySet()) {
			Files.createDirectories(Paths.get(OUTPUT_FODLER+"/partitionId="+centerId));
			outputFiles.put(centerId,Files.newBufferedWriter(Paths.get(OUTPUT_FODLER+"/partitionId="+centerId+"/words")));
		}
		
		Set<BufferedWriter> usedWriters = new HashSet<>();
		
		getInputData().forEachOrdered((w) -> {
			int center = getNearestCluster(w.getVector(), finalCenters);
			String line = w.getWord()+Constants.ELEMENT_SEPARATOR+w.getVector();
			try {
				if(usedWriters.contains(allWordsFile)) {
					allWordsFile.newLine();					
				} else {
					usedWriters.add(allWordsFile);
				}
				allWordsFile.write(center+Constants.ELEMENT_SEPARATOR+line);
				
				BufferedWriter outputFile = outputFiles.get(center);
				if(usedWriters.contains(outputFile)) {
					outputFile.newLine();					
				} else {
					usedWriters.add(outputFile);
				}
				outputFile.write(line);
			} catch (Exception e1) {
				throw new RuntimeException(e1);
			}
			
		});
		
		
		allWordsFile.close();
		outputFiles.values().stream().forEach((b) ->{ try {b.close();}catch(Exception e){}});
		
		System.out.println("done");
		
	}
	
	public static int getNearestCluster(Vector vec, Map<Integer,Vector> clusters) {
		double minDist = Double.MAX_VALUE;
		int nearest = 0;
		for(Entry<Integer,Vector> e : clusters.entrySet()) {
			double dist = e.getValue().cosineDistance(vec);
			if(dist < minDist) {
				minDist = dist;
				nearest = e.getKey();
			}
		}
		return nearest;
	}

	public static Map<Integer, Vector> getInitCenters(int workingSetSize, int k, long rndSeed) {
		Random rnd = new Random(rndSeed);

		long numWords = getInputData().count();

		Set<Long> workingSetIndices = Stream.generate(() -> (long) (rnd.nextDouble() * numWords)).distinct()
				.limit(workingSetSize)
				.collect(Collectors.toSet());

		AtomicLong index = new AtomicLong(0);

		List<Vector> workingSet = getInputData()
				.filter((w) -> workingSetIndices.contains(index.getAndIncrement()))
				.map(Word::getVector)
				.collect(Collectors.toList());

		List<Vector> clusterCenters = new ArrayList<>();
		clusterCenters.add(workingSet.remove(workingSetSize - 1));

		// initialize the distances
		Map<Vector, Double> minimumSeedDistance = new HashMap<>();
		for (Vector vec : workingSet) {
			minimumSeedDistance.put(vec, vec.cosineDistance(clusterCenters.get(0)));
		}
		// add the one with the biggest minimum distance to the set of centers
		while (clusterCenters.size() < k) {
			// scan for the maximum distance
			Entry<Vector, Double> maximum = null;
			for (Entry<Vector, Double> ent : minimumSeedDistance.entrySet()) {
				if (maximum == null || ent.getValue() > maximum.getValue()) {
					maximum = ent;
				}
			}
			// add it to the cluster centers;
			Vector newCenter = maximum.getKey();
			clusterCenters.add(newCenter);
			minimumSeedDistance.remove(newCenter);
			// update the distance map
			for (Vector vec : minimumSeedDistance.keySet()) {
				double distance = minimumSeedDistance.get(vec);
				distance = Math.min(distance, vec.cosineDistance(newCenter));
				minimumSeedDistance.put(vec, distance);
			}
		}

		Map<Integer, Vector> result = new HashMap<>();
		for (int i = 0; i < clusterCenters.size(); i++) {
			result.put(i+1, clusterCenters.get(i));
		}
		return result;

	}
	
	public static void removeRecursive(Path path) throws IOException
	{
	    Files.walkFileTree(path, new SimpleFileVisitor<Path>()
	    {
	        @Override
	        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
	                throws IOException
	        {
	            Files.delete(file);
	            return FileVisitResult.CONTINUE;
	        }

	        @Override
	        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
	        {
	            // try to delete the file anyway, even if its attributes
	            // could not be read, since delete-only access is
	            // theoretically possible
	            Files.delete(file);
	            return FileVisitResult.CONTINUE;
	        }

	        @Override
	        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
	        {
	            if (exc == null)
	            {
	                Files.delete(dir);
	                return FileVisitResult.CONTINUE;
	            }
	            else
	            {
	                // directory iteration failed; propagate exception
	                throw exc;
	            }
	        }
	    });
	}

}
