package udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class SubVec extends UDF{

	public ArrayList<Double> vecSub(ArrayList<Double> v1, ArrayList<Double> v2){
		ArrayList<Double> r = new ArrayList<Double>(v1.size());
		for (int i = 0; i < v1.size(); i++) {
			r.set(i, v1.get(i)-v2.get(i));
		} 
		return r;
	}
}
