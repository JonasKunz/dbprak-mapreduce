package io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import model.ClusterCenter;
import model.Vector;

public class WordVectorRecordReader implements RecordReader<Text, Vector> {

	private LineRecordReader reader;
	
	public WordVectorRecordReader(LineRecordReader reader){
		this.reader = reader;
	}
			
	public void close() throws IOException {
		reader.close();
	}

	public Text createKey() {
		return new Text();
	}

	public Vector createValue() {
		return new Vector();
	}

	public long getPos() throws IOException {
		return reader.getPos();
	}

	public float getProgress() throws IOException {
		return reader.getProgress();
	}

	public boolean next(Text word, Vector vec) throws IOException {
		LongWritable lineNumber = new LongWritable();
		Text rawText = new Text();
		if(!reader.next(lineNumber, rawText)) {
			return false;
		} else {
			String[] splitted = rawText.toString().split(":");		
			word.set(splitted[0]);
			
			String[] vecComponents = splitted[1].split(",");
			double[] vecData = new double[vecComponents.length];
			for(int i=0; i<vecComponents.length; i++) {
				vecData[i] = Double.parseDouble(vecComponents[i]);
			}
			vec.setSubVector(0, vecData);
			return true;
		}
	}

}
