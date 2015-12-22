package model;

import org.apache.commons.math.FunctionEvaluationException;
import org.apache.commons.math.analysis.UnivariateRealFunction;
import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math3.stat.StatUtils;

public class Vector extends ArrayRealVector {
	
	public double cosineDistance(Vector v) throws IllegalArgumentException {
		double cosSimilarity = this.dotProduct(v) / (v.getNorm() * this.getNorm());
		return 1- cosSimilarity;
	}

	public Vector(){
		super();
	}
	
	public Vector(double[] d){
		super(d);
	}
	
	public Vector(ArrayRealVector arv){
		super(arv);
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
