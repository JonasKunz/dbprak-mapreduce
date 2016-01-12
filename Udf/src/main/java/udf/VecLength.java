package udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class VecLength extends UDF{

	public Double evaluate(ArrayList<Double> v1){
		VecScalar vs = new VecScalar();
		return (Double) Math.sqrt(vs.evaluate(v1, v1));		 
	}
	
}
