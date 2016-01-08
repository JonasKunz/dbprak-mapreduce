package udfHive;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveUDF extends UDF{
	public ArrayList<Float> vecAdd(ArrayList<Float> v1, ArrayList<Float> v2){
		ArrayList<Float> r = new ArrayList<Float>(v1.size());
		for (int i = 0; i < v1.size(); i++) {
			r.set(i, v1.get(i)+v2.get(i));
		} 
		return r;
	}
	
	public ArrayList<Float> vecSub(ArrayList<Float> v1, ArrayList<Float> v2){
		ArrayList<Float> r = new ArrayList<Float>(v1.size());
		for (int i = 0; i < v1.size(); i++) {
			r.set(i, v1.get(i)-v2.get(i));
		} 
		return r;
	}
	
	public float vecScalar(ArrayList<Float> v1, ArrayList<Float> v2){
		float sum = 0;
		for (int i = 0; i < v1.size(); i++) {
			sum += v1.get(i)*v2.get(i);
		} 
		return sum;
	}
	
	public ArrayList<Float> vecNegation(ArrayList<Float> v1){
		for (int i = 0; i < v1.size(); i++) {
			v1.set(i, -v1.get(i));
		} 
		return v1;
	}
	
	public float vecLength(ArrayList<Float> v1){
		return (float) Math.sqrt(vecScalar(v1, v1));		 
	}
	
	public float vecDistance(ArrayList<Float> v1, ArrayList<Float> v2){
		return vecScalar(v1, v2)/(vecLength(v1)*vecLength(v2));
	}
}
