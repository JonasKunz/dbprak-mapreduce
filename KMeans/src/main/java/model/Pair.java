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
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((frst == null) ? 0 : frst.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (frst == null) {
			if (other.frst != null)
				return false;
		} else if (!frst.equals(other.frst))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}
	
	

}
