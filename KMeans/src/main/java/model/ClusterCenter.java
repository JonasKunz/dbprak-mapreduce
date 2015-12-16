package model;

public class ClusterCenter extends Vector{
	
	public ClusterCenter(double[] d) {
		super(d);
	}

	private int number;

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}
}
