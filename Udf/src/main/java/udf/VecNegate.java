package udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class VecNegate extends UDF{

	public ArrayList<Double> evaluate(ArrayList<Double> v1){
		for (int i = 0; i < v1.size(); i++) {
			v1.set(i, -v1.get(i));
		} 
		return v1;
	}
}
