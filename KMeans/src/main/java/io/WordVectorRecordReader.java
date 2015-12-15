package io;

import java.io.IOException;

import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import model.ClusterCenter;

public class WordVectorRecordReader<K,V> implements RecordReader<K,V> {

	private LineRecordReader reader;
	
	public WordVectorRecordReader(LineRecordReader reader){
		this.reader = reader;
	}
			
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public K createKey() {
		// TODO Auto-generated method stub
		return null;
	}

	public V createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean next(K arg0, V arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
