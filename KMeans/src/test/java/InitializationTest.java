import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.jets3t.service.io.InputStreamWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.HDFSAccessor;
import main.Initializer;

public class InitializationTest {

	String inputFolder = "testData";
	String outputFolder = "testData\\output";

	private static FileSystem fs;
	
	@BeforeClass
	public static void initFakeHDFS() {
		try {
			fs = FileSystem.getLocal(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Before
	public void createOutputDir() {
		new File(outputFolder).mkdir();
	}
	
	@After
	public void clearOutput() {
		deleteFolder(new File(outputFolder));
	}
	
	public static void deleteFolder(File folder) {
	    File[] files = folder.listFiles();
	    if(files!=null) { //some JVMs return null for empty dirs
	        for(File f: files) {
	            if(f.isDirectory()) {
	                deleteFolder(f);
	            } else {
	                f.delete();
	            }
	        }
	    }
	    folder.delete();
	}
	
	@Test
	public void testFileAccess() {
		String inUri = new File(inputFolder+"\\inputWords.txt").getAbsoluteFile().toURI().toString();
		String outUri = new File(outputFolder+"\\outputCenters.txt").getAbsoluteFile().toURI().toString();
		HDFSAccessor acc = new HDFSAccessor(fs);
		
		Initializer init = new Initializer(acc, inUri);
		init.computeCenters(2, 1, 0, outUri);
		
		acc.readFile(outUri, (ln,l) -> {
			System.out.println("center : " + l);
		});
	}

}
