package udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class VecScalar extends UDF{

	public Double evaluate(ArrayList<Double> v1, ArrayList<Double> v2){
		double sum = 0;
		for (int i = 0; i < v1.size(); i++) {
			sum += v1.get(i)*v2.get(i);
		} 
		return sum;
	}

}
