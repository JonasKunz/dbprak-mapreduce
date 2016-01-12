package udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class AddVec extends UDF{

	public ArrayList<Double> evaluate(ArrayList<Double> d, ArrayList<Double> d1){
		ArrayList<Double> r = new ArrayList<Double>();
		for (int i = 0; i < d1.size(); i++) {
			r.add(i, d.get(i)+d1.get(i));
		}
		return r;
	}
}
