package io;

public class WriterUtil {

	private StringBuffer sb;
	
	private String separator;
	
	
	
	public WriterUtil(String separator) {
		super();
		sb = new StringBuffer();
		this.separator = separator;
	}


	public void writeString(String str) {
		appendSeparator();
		sb.append(str);
	}
	
	public void writeInt(int i) {
		appendSeparator();
		sb.append(Integer.toString(i));
	}
	
	public void writeLong(long i) {
		appendSeparator();
		sb.append(Long.toString(i));
	}
	
	public void writeDouble(double i) {
		appendSeparator();
		sb.append(Double.toString(i));
	}
	

	public void writeDoubleArray(double[] arr, String arraySeperator) {
		appendSeparator();
		for(int i=0; i<arr.length; i++) {
			if (i != 0) {
				sb.append(arraySeperator);
			}
			sb.append(Double.toString(arr[i]));
		}
	}
	
	private void appendSeparator() {
		if(sb.length() != 0) {
			sb.append(separator);
		}
	}

	@Override
	public String toString() {
		return  sb.toString();
	}
	
	
	
}
