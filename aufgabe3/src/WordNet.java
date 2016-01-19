package main;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import edu.smu.tspell.wordnet.NounSynset;
import edu.smu.tspell.wordnet.Synset;
import edu.smu.tspell.wordnet.SynsetType;
import edu.smu.tspell.wordnet.WordNetDatabase;

public class WordNet {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		try {
		      Class.forName(driverName);
		    } catch (ClassNotFoundException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		    System.exit(1);
		  }
		Connection con = DriverManager.getConnection("jdbc:hive2://meereen.ipd.kit.edu:10000/default", "stupidusername", "stupidpw");
		System.out.println(con.getSchema());
		Statement stmt = con.createStatement();
		String tableName = "word2vecb";
		String sql = "show tables";
		//String sql = "select * from " + tableName + " limit 5";
		//String sql = "ALTER TABLE word2vecb ADD PARTITION(partionId='1')";
		/*String sql = "CREATE EXTERNAL TABLE `Word2VecB`( "+
					"id INT, " +
					"name String, " +
					"vector array<float>) " +
					"PARTITIONED BY ( " +
					"partionId INT) " +
					"ROW FORMAT DELIMITED " +
					"FIELDS TERMINATED BY ':' " +
					"COLLECTION ITEMS TERMINATED BY ',' " +
					"STORED AS INPUTFORMAT " +
					"'org.apache.hadoop.mapred.TextInputFormat' " +
					"OUTPUTFORMAT " +
					"'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
					"LOCATION " +
					"'hdfs://hadoopmaster:9000/usr/local/hadoop/dbprak/group2/words2vec'";*/
		/*String sql = "CREATE EXTERNAL TABLE ClusterCenterB( " +
				"partionId INT, " +
				"vector array<float>) " +
				"ROW FORMAT DELIMITED " +
				"FIELDS TERMINATED BY ':' " +
				"COLLECTION ITEMS TERMINATED BY ',' " +
				"STORED AS INPUTFORMAT " +
				"'org.apache.hadoop.mapred.TextInputFormat' " +
				"OUTPUTFORMAT " +
				"'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
				"LOCATION " +
				"'hdfs://hadoopmaster:9000/usr/local/hadoop/dbprak/group2/clusterCenter' ";*/
		//System.out.println("Running: " + sql);
		//res = stmt.executeQuery(sql);
		//if (res.next()) {
		//  System.out.println(res.getString(1));
		//}
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
		 // System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
			System.out.println(res.getString(1));
		}
		
		NounSynset nounSynset;
		NounSynset[] hyponyms;

		/*WordNetDatabase database = WordNetDatabase.getFileInstance();
		Synset[] synsets = database.getSynsets("fly", SynsetType.NOUN);
		for (int i = 0; i < synsets.length; i++) {
		    nounSynset = (NounSynset)(synsets[i]);
		    hyponyms = nounSynset.getHyponyms();
		    System.err.println(nounSynset.getWordForms()[0] +
		            ": " + nounSynset.getDefinition() + ") has " + hyponyms.length + " hyponyms");
		} */
		

		
	}
	

}
