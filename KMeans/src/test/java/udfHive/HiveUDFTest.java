package udfHive;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import model.Vector;

public class HiveUDFTest {

	@Test
	public void test() {
		HiveUDF udf = new HiveUDF();
		ArrayList<Float> v1 = new ArrayList<Float>();
		v1.add((float) 1);
		v1.add((float) 2);
		v1.add((float) 3);
		ArrayList<Float> v2 = new ArrayList<Float>();
		v2.add((float) 3);
		v2.add((float) 4);
		v2.add((float) 5);
		System.out.println(udf.vecDistance(v1, v2));
		
		assertEquals(udf.vecDistance(v1, v2),0.9827076,0.1);
		
	}

}
