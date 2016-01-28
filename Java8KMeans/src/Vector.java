
public class Vector {

	private final double[] data;
	
	public Vector(double[] data) {
		this.data = data;
	}

	public Vector add(Vector otherVector) {
		checkEqualDimension(otherVector);
		double[] result = new double[data.length];
		for(int i=0; i<data.length; i++) {
			result[i] = data[i] + otherVector.data[i];
		}
		return new Vector(result);
	}
	
	public double cosineDistance(Vector otherVector) {
		checkEqualDimension(otherVector);
		return 1.0 - dot(otherVector) / this.length() / otherVector.length();
	}

	public double dot(Vector otherVector){
		checkEqualDimension(otherVector);
		double dot = 0;
		for(int i=0; i<data.length; i++) {
			dot += data[i]*otherVector.data[i];
		}
		return dot;
	}
	
	public double length(){
		return Math.sqrt(dot(this));
	}
	
	public Vector getNormalized(){
		double invLen = 1.0 / length();
		double[] result = new double[data.length];
		for(int i=0; i<data.length; i++) {
			result[i] = data[i] * invLen;
		}
		return new Vector(result);		
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(double val : data) {
			if(sb.length() > 0) {
				sb.append(Constants.ARRAY_SEPARATOR);
			}
			sb.append(val);
		}
		return sb.toString();
	}
	
	
	private void checkEqualDimension(Vector otherVector) {
		if(otherVector.data.length != data.length) {
			throw new IllegalArgumentException("Vectors have different Dimensions!");
		}
	}

	public int getDimension() {
		return data.length;
	}
	
	
}
