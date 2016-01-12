package io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import model.Vector;

public class WordVectorInputFormat extends FileInputFormat<Text, Vector> {

    private TextInputFormat tp = new TextInputFormat();
    
    @Override
    public RecordReader<Text, Vector> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
	    InterruptedException {
	LineRecordReader recordReader = (LineRecordReader) tp.createRecordReader(arg0,arg1);
	return new WordVectorRecordReader(recordReader);
    }

    public List<InputSplit> getSplits(JobContext arg0) throws IOException {
	return tp.getSplits(arg0);
    }	
    
    
}
