package main;

import java.sql.SQLException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WordNet {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String jarLocation = "hdfs://hadoopmaster:9000/usr/local/hadoop/dbprak/group2/Udf.jar"; 
	private static String Word2VecTableName = "word2vecb";
	private static String ClusterTableName = "clustercenterb";
	private static Statement stmt = null;
	private static Connection con = null;
	
	public static void main(String args[]){
		
		try {
			Map<Integer, Map<RelationType,Double>> result = new HashMap<Integer, Map<RelationType,Double>>();
			//initializing database connection
			try {
			      Class.forName(driverName);
			    } catch (ClassNotFoundException e) {
			    e.printStackTrace();
			    System.exit(1);
			  }
			con = DriverManager.getConnection("jdbc:hive2://meereen.ipd.kit.edu:10000/default", "stupidusername", "stupidpw");
			stmt = con.createStatement();
			String sql;
			ArrayList<Integer> clusterList = new ArrayList<Integer>();
			ResultSet res;

			sql = "select PartitionId from "
					+ ClusterTableName;
			res = stmt.executeQuery(sql);
			while (res.next()) {
				//saving the cluster IDs
				int clusterID = Integer.parseInt(res.getString(1));
				clusterList.add(clusterID);
			}
			
			//adding the jar file
			sql = "ADD JAR " + jarLocation;
			stmt.execute(sql);
			
			for (Integer clusterID : clusterList) {
				//saving the results
				result.put(clusterID, evaluateCluster(clusterID));
				System.out.println(clusterID);
			}
			con.close();
			

			BufferedWriter writer = null;

			writer = new BufferedWriter(new FileWriter(new File("results.txt")));
			
			//writing the results into a text file
			for (Map.Entry<Integer, Map<RelationType,Double>> cluster : result.entrySet()) {
				writer.write(cluster.getKey());
				writer.write(" ");
				writer.write(cluster.getValue().get(RelationType.ANTONYM).toString());
				writer.write(" ");
				writer.write(cluster.getValue().get(RelationType.HYPERNYM).toString());
				writer.write(" ");
				writer.write(cluster.getValue().get(RelationType.HYPONYM).toString());
				writer.write(" ");
				writer.write(cluster.getValue().get(RelationType.SYNONYM).toString());
				writer.write(" ");
				writer.write(cluster.getValue().get(RelationType.NO_RELATION).toString());
				writer.write("\n");
			}
			
			writer.close();
			
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}				
	}
	
	//evaluate the cluster with the corresponding ID
	public static Map<RelationType, Double> evaluateCluster(int clusterID) throws SQLException
	{
		//declarations
		Vector closestVector = null;
		String closestWord = null;
		int vectorCount = 0;
		Map<RelationType, Integer> counts = new HashMap<RelationType, Integer>();
		Map<RelationType, Double> results = new HashMap<RelationType, Double>();
		
		String sql;
		ResultSet res;
		
		//reading the data of the word2vec table into vectors
		//and determining the vector that is the closest to the center
		sql = "select " + Word2VecTableName + ".name, " + 
						  Word2VecTableName + ".vector, " + 
						  "vecDistance(" + Word2VecTableName + ".vector, "
						  + ClusterTableName + ".vector) as distance" + " " +
			  "from " + Word2VecTableName + " " +
			  "left outer join " + ClusterTableName + " " +
			  "on( " + Word2VecTableName + ".partitionid = " + 
			           ClusterTableName + ".partitionid) " + 
			  "where " + ClusterTableName + ".partitionid = " + clusterID + " " +
			  "order by distance asc " +
			  "limit 1";
		res = stmt.executeQuery(sql);
		res.next();
		closestWord = res.getString(1);
		closestVector = new Vector(VectorParser.Parse(res.getString(2)));
		res.close();
		
		//fetching and storing the relations for each vector
		WordNetSearch wns = new WordNetSearch();
		
		sql = "select name " +
			  "from " + Word2VecTableName + " " +
			  "where partitionid = " + clusterID;
		res = stmt.executeQuery(sql);
		while(res.next()){
			String word = res.getString(1);
			vectorCount++;
			if(!closestWord.equals(word)){
				RelationType relationType = wns.GetRelationType(closestWord, word);
				counts.replace(relationType, counts.get(relationType) + 1);
			}
		}
		
		//determining the proportion of the relations
		for (RelationType relType : counts.keySet()) {
			results.put(relType, ((double)(counts.get(relType))/vectorCount));
		}
		
		return results;
	}

}
