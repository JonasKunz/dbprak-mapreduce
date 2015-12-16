package model;

import org.apache.commons.math.FunctionEvaluationException;
import org.apache.commons.math.analysis.UnivariateRealFunction;
import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math3.stat.StatUtils;

public class Vector extends ArrayRealVector {
	
	public double dotProduct(Vector v) throws IllegalArgumentException {
//		this.normalize();
//		v.normalize();
		return super.dotProduct(v);
	}

	private ClusterCenter associatedCluster;
	
	public Vector(double[] d){
		super(d);
	}
	
	public static Vector getInitialisedVector(String line) throws NumberFormatException{
		String[] split = line.split(",");
		double[] vectorPoint;
		vectorPoint = new double[split.length];
		for (int i = 0; i < split.length; i++) {
			vectorPoint[i] = Double.parseDouble(split[i]);
		}
		return new Vector(vectorPoint);
	}
	
	public ClusterCenter getAssociatedCluster() {
		return associatedCluster;
	}
	public void setAssociatedCluster(ClusterCenter associatedCluster) {
		this.associatedCluster = associatedCluster;
	}	
	
	public void normalize(){
		this.setSubVector(0, StatUtils.normalize(this.getData()));
	}
	
	public void negate(){
		try {
			this.setSubVector(0, this.map(new UnivariateRealFunction() {
				public double value(double x) throws FunctionEvaluationException {
					return -x;
				}
			}).getData());
		} catch (FunctionEvaluationException e) {
			e.printStackTrace();
		}
	}

	
	



		
}
