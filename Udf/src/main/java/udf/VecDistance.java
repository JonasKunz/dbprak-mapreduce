package udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class VecDistance extends UDF {

	
	public Double evaluate(ArrayList<Double> v1, ArrayList<Double> v2){
		VecScalar vs = new VecScalar();
		VecLength vl = new VecLength();
		return vs.evaluate(v1, v2)/(vl.evaluate(v1)*vl.evaluate(v2));
	}
}
