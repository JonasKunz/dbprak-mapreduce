package main;

public class VectorParser {
	public static Double[] Parse(String input){
		String coords = input.substring(1, input.length()-2);
		String[] coords_split = coords.split(",");
		Double[] result = new Double[coords_split.length];
		for (int i = 0; i < coords_split.length; i++) {
			result[i] = Double.parseDouble(coords_split[i]); 
		}
		return result;
	}
}
