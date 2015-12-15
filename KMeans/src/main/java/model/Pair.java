package model;

public class Pair<K, V> {
	
	private K frst;
	private V second;
	public Pair(K frst, V second) {
		super();
		this.frst = frst;
		this.second = second;
	}
	public K getFrst() {
		return frst;
	}
	public void setFrst(K frst) {
		this.frst = frst;
	}
	public V getSecond() {
		return second;
	}
	public void setSecond(V second) {
		this.second = second;
	}
	

}
