package model;

public class Vector {
	private float[] vectorPoint;
	private ClusterCenter associatedCluster;
	
	public Vector(){
		
	}
	
	public Vector(float[] vectorPoint){
		this.vectorPoint = vectorPoint;
	}
	
	public Vector(float[] vectorPoint, ClusterCenter associatedCluster){
		this.vectorPoint = vectorPoint;
		this.associatedCluster = associatedCluster;
			
	}
	
	public void initVector(String line) throws NumberFormatException{
		String[] split = line.split(",");
		vectorPoint = new float[split.length];
		for (int i = 0; i < split.length; i++) {
			vectorPoint[i] = Float.parseFloat(split[i]);
		}
	}
	
	public ClusterCenter getAssociatedCluster() {
		return associatedCluster;
	}
	public void setAssociatedCluster(ClusterCenter associatedCluster) {
		this.associatedCluster = associatedCluster;
	}	
	
	public float[] getVectorPoint() {
		return vectorPoint;
	}

	public void setVectorPoint(float[] vectorPoint) {
		this.vectorPoint = vectorPoint;
	}
	
	public Vector vectorAdd(Vector v1, Vector v2){
		float[] vec1 = v1.getVectorPoint();
		float[] vec2 = v2.getVectorPoint();
		assert vec1.length == vec2.length;
		float[] vecR = new float[vec1.length];
		for (int i = 0; i < vec2.length; i++) {
			vecR[i] = vec1[i]+vec2[i];
		}
		return new Vector(vecR);
	}

		
}
