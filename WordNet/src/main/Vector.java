package main;

import java.util.ArrayList;
import java.util.Arrays;

public class Vector {
	public ArrayList<Double> coords;
	
	Vector()
	{
		coords = new ArrayList<Double>();
	}
	
	Vector(Double[] coords_array){
		coords = new ArrayList<Double>(Arrays.asList(coords_array));
	}
}
