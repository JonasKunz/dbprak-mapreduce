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

    public void assignCenters(List<ClusterCenter> centers, String outputPath) {
	for (ClusterCenter center : centers) {
	    Iterator<String> it = hdfs.readFile(inputPath);
	    ParsingUtil pu = new ParsingUtil(ELEMENT_SEPARATOR);
	    hdfs.clearAndWriteFile(outputPath + center.getNumber().get() + "/words", (lnr) -> {
		String lineToWrite = null;
		while  (lineToWrite == null && it.hasNext()) {
		    String originalLine = it.next();
		    StringBuffer line = new StringBuffer(originalLine);
		    String word = pu.parseString(line); // parse the word
		    double[] vectorValues = pu.parseDoubleArray(line, ARRAY_SEPARATOR);
		    Vector vector = new Vector(vectorValues);
		    ClusterCenter closest = null;
		    double closestDist = Double.MAX_VALUE;
		    for (ClusterCenter cluster : centers) {
			double dist = cluster.cosineDistance(vector);
			if (dist < closestDist) {
			    closestDist = dist;
			    closest = cluster;
			}
		    }
		    if (closest.equals(center)) {
			lineToWrite = originalLine;
		    }
		}
		return lineToWrite;
	    });

	}

    }
}
