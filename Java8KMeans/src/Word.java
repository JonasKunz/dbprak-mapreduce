import java.util.Optional;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

public class Word {
	private Vector vector;
	private String word;
	public Word(Vector vector, String word) {
		super();
		this.vector = vector;
		this.word = word;
	}
	public Vector getVector() {
		return vector;
	}
	public void setVector(Vector vector) {
		this.vector = vector;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	
	public static Optional<Word> parse(String line) {
		String[] parts = line.split(Constants.ELEMENT_SEPARATOR);
		if(parts.length != 2) {
			return Optional.empty();
		} else {
			try {
				String word = parts[0];
				String[] valuesText = parts[1].split(Constants.ARRAY_SEPARATOR);
				double[] values = Stream.<String>of(valuesText).mapToDouble(Double::parseDouble).toArray();
				return Optional.of(new Word(new Vector(values), word));
			} catch(NumberFormatException e) {
				return Optional.empty();				
			}
		}
	}
	
}
