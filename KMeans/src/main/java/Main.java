import model.ClusterCenter;
import util.Parser;


public class Main {

    public static void main(String[] args) {

	ClusterCenter c1 = new ClusterCenter(new double[]{5.0,5.0,6.0});
	ClusterCenter c2 = new ClusterCenter(new double[]{4.0,2.0,1.0});
	ClusterCenter[] arrayC = {c1,c2};
	
	System.out.println(Parser.arraytoString(arrayC));
    }

}
