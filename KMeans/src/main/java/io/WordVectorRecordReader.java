package io;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import model.Vector;

public class WordVectorRecordReader extends RecordReader<Text, Vector> {

    private LineRecordReader reader;
    private Text word;
    private Vector vec;

    public WordVectorRecordReader(LineRecordReader reader) {
	this.reader = reader;
    }

    @Override
    public void close() throws IOException {
	reader.close();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
	return word;
    }

    @Override
    public Vector getCurrentValue() throws IOException, InterruptedException {
	return vec;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
	return reader.getProgress();
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
	reader.initialize(arg0, arg1);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
	
	if (!reader.nextKeyValue()) {
	    word = null;
	    vec = null;
	    return false;
	} else {
	    Text rawText = reader.getCurrentValue();
	    String[] splitted = rawText.toString().split(":");
	    word.set(splitted[0]);

	    String[] vecComponents = splitted[1].split(",");
	    double[] vecData = new double[vecComponents.length];
	    for (int i = 0; i < vecComponents.length; i++) {
		vecData[i] = Double.parseDouble(vecComponents[i]);
	    }
	    vec.setSubVector(0, vecData);
	    return true;
	}
    }
    
    

}
