package io;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import model.Vector;

public class WordVectorInputFormat extends FileInputFormat<Text, Vector> {

	private TextInputFormat tp = new TextInputFormat();

	public void configure(JobConf conf) {
		tp.configure(conf);
	}

	public RecordReader<Text, Vector> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {
		LineRecordReader recordReader = (LineRecordReader) tp.getRecordReader(genericSplit, job, reporter);
		return new WordVectorRecordReader(recordReader);
	}

	public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
		return tp.getSplits(arg0, arg1);
	}

	
	
	
	
}
