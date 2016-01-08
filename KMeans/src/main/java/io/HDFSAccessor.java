package io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSAccessor {

	public void readFile(String path, BiConsumer<Long, String> lineConsumer) {
		Path pt=new Path(path);
        FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	        String line;
	        long lineNumber = 1;
	        while((line = br.readLine()) != null) {
	        	lineConsumer.accept(lineNumber, line);
	        	lineNumber++;
	        }
	        br.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void clearAndWriteFile(String path, Function<Long, String> lineProducer) {
		Path pt=new Path(path);
        FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			long lineNumber = 1;
			String line;
			while ((line = lineProducer.apply(lineNumber)) != null){
				if(lineNumber != 1){
					bw.newLine();
				}
				bw.write(line);
				lineNumber++;
			}
			bw.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
}
