package io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSAccessor {


	FileSystem fs;

	public HDFSAccessor(FileSystem fs) {
		super();
		this.fs = fs;
	}

	public HDFSAccessor() {
		super();
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public HDFSAccessor(Configuration config) {
		super();
		try {
			fs = FileSystem.get(config);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void readFile(String path, BiConsumer<Long, String> lineConsumer) {
		Path pt = new Path(path);
		try {
			FSDataInputStream inputStream = null;
			if (fs.exists(pt)) {
				inputStream = fs.open(pt);
			} else {
				System.out.println("File does not exist" + path);
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			String line;
			long lineNumber = 1;
			while ((line = br.readLine()) != null) {
				lineConsumer.accept(lineNumber, line);
				lineNumber++;
			}
			br.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Iterator<String> readFile(String path) {
		Path pt = new Path(path);
		try {
			FSDataInputStream inputStream = null;
			if (fs.exists(pt)) {
				inputStream = fs.open(pt);
			} else {
				System.out.println("File does not exist" + path);
			}
			final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			Iterator<String> it = new FileIterator(br);
			return it;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	

	private final class FileIterator implements Iterator<String> {
		
		private final BufferedReader br;
		
		private String currentLine = null;
		private boolean isClosed = false;
		
		private FileIterator(BufferedReader br) {
			this.br = br;
			readNextLine();
		}

		private void readNextLine() {
			if(isClosed) {
				currentLine = null;
			} else {
				try {
					currentLine = br.readLine();
					if(currentLine == null) {
						br.close();
						isClosed = true;
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}

		@Override
		public boolean hasNext() {
			return currentLine != null;
		}

		@Override
		public String next() {
			String lastLine = currentLine;
			readNextLine();
			return lastLine;
		}
	}

	public void clearAndWriteFile(String path, Function<Long, String> lineProducer) {
		Path pt = new Path(path);
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
			long lineNumber = 1;
			String line;
			while ((line = lineProducer.apply(lineNumber)) != null) {
				if (lineNumber != 1) {
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
