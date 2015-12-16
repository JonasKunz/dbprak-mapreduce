package model;

import static org.junit.Assert.*;

import org.apache.commons.math.linear.ArrayRealVector;
import org.junit.Test;

import model.Vector;

public class VectorTest {

	@Test
	public void test() {
		Vector v1 = new Vector(new double[]{1.0,2.0,3.0,4.9,5.1});
		Vector v2 = new Vector(new double[]{1.0,2.0,3.0,4.9,20});
		Vector v3 = new Vector(v1.subtract(v2).getData());
		assertArrayEquals(new double[]{0, 0, 0, 0, -14.9},v3.getData(),0.1);
		v1 = new Vector(new double[]{0.5,0.1});
		v1.normalize();
		assertTrue(v1.getNorm()==1);
		v1 = new Vector(new double[]{1,2,3});
		v2 = new Vector(new double[]{3,4,5});
		assertTrue(v1.dotProduct(v2)==26);
		v1.negate();
		assertArrayEquals(new double[]{-1,-2,-3}, v1.getData(), 0.1);
		
	}

}
