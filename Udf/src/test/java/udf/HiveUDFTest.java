package udf;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;



public class HiveUDFTest {

	@Test
	public void test() {
		ArrayList<Double> v1 = new ArrayList<Double>();
		v1.add((double)1);
		v1.add((double)2);
		v1.add((double)3);
		ArrayList<Double> v2 = new ArrayList<Double>();
		v2.add((double) 3);
		v2.add((double) 4);
		v2.add((double) 5);
		AddVec ad = new AddVec();
		System.out.println(ad.evaluate(v1, v2));
	//	System.out.println(udf.vecDistance(v1, v2));
		
	//	assertEquals(udf.vecDistance(v1, v2),0.9827076,0.1);
		
	}

}
