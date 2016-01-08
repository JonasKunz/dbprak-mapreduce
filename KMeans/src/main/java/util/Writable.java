package util;

public class Writable<T> {
	
	private T value;

	public Writable(T value) {
		this.value = value;
	};
	
	public void set(T val) {
		value = val;
	}
	
	public T get() {
		return value;
	}
	
	

}
