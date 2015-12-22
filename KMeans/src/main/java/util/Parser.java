package util;

import model.ClusterCenter;

public class Parser {
    
    public static String arraytoString(ClusterCenter[] clustercenters){
	String result = "";
	for (ClusterCenter clusterCenter : clustercenters) {
	    result += clusterCenter.getNumber() +":"; 
	    double[] clusterData = clusterCenter.getData();
	    for(int i = 0; i < clusterData.length -1; i++){
		result += clusterData[i] +",";
	    }
	    result += clusterData[clusterData.length-1] + "\n";
	}
	return result;	
    }
}
