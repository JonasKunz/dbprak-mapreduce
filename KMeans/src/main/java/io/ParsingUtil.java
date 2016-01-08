package io;

import java.util.ArrayList;

public class ParsingUtil {
	
	private String separator;
	
	 
	
	public ParsingUtil(String separator) {
		super();
		this.separator = separator;
	}

	public int parseInteger(StringBuffer sb) {
		String arg = takeArgument(sb);
		return Integer.parseInt(arg);
	}
	
	public String parseString(StringBuffer sb) {
		String arg = takeArgument(sb);
		return arg;
	}
	
	public float parseFloat(StringBuffer sb) {
		String arg = takeArgument(sb);
		return Float.parseFloat(arg);
	}
	
	public float[] parseFloatArray(StringBuffer sb, String elementSeparator) {
		ParsingUtil arrParser =new ParsingUtil(elementSeparator);
		StringBuffer arg = new StringBuffer(takeArgument(sb));
		int count = arrParser.getArgumentsCount(arg);
		float[] arrayData = new float[count];
		for(int i=0; i<count; i++) {
			arrayData[i] = arrParser.parseFloat(arg);
		}
		return arrayData;
	}
	
	public double parseDouble(StringBuffer sb) {
		String arg = takeArgument(sb);
		return Double.parseDouble(arg);
	}
	
	public double[] parseDoubleArray(StringBuffer sb, String elementSeparator) {
		ParsingUtil arrParser =new ParsingUtil(elementSeparator);
		StringBuffer arg = new StringBuffer(takeArgument(sb));
		int count = arrParser.getArgumentsCount(arg);
		double[] arrayData = new double[count];
		for(int i=0; i<count; i++) {
			arrayData[i] = arrParser.parseDouble(arg);
		}
		return arrayData;
	}
	
	public int getArgumentsCount(StringBuffer arg) {
		if(arg.length() == 0){
			return 0;
		} else {
			int count = 1;
			int indx = arg.indexOf(separator, 0);
			while(indx != -1) {
				count++;
				indx = arg.indexOf(separator, indx+1);
			}
			return count;
		}
	}

	private String takeArgument(StringBuffer sb) {
		int sepIndex = sb.indexOf(separator);
		String arg;
		if(sepIndex == -1) {
			arg = sb.toString();
			sb.setLength(0);
		} else {
			arg = sb.substring(0, sepIndex);
			sb.delete(0, sepIndex + separator.length());
		}
		return arg;
	}
	

}
