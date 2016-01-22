import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


public class TableLoader {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String connectionURL = "jdbc:hive2://meereen.ipd.kit.edu:10000/default";
	private static Connection connection = null;
	private static Statement statement = null;
	private static String word2VecTableName = "Word2VecB";
	private static String word2VecLocation = "hdfs://hadoopmaster:9000/usr/local/hadoop/dbprak/group2/output1";
	private static String clusterCenterTableName = "ClusterCenterB";
	private static String clusterCenterLocation = "hdfs://hadoopmaster:9000/usr/local/hadoop/dbprak/group2/output1/clusterCenter";
	
	
	public static void main(String[] args) {
		//connecting to database
		try {
			connect();
		} catch (ClassNotFoundException e) {
			System.err.println("Could not load JDBC driver!");
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			System.err.println("Could not connect to database!");
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Database accessed successfully!");
		
		//creating the sql statement
		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			System.err.println("Could not create statement!");
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("SQL statement created successfully!");
		
		//dropping tables if they exist
		try {
			dropTables();
		} catch (SQLException e) {
			System.err.println("Could not drop tables!");
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Former tables dropped successfully!");
		
		//creating the ClusterCenter table
		try {
			createClusterCenterTable();
		} catch (SQLException e) {
			System.err.println("Could not create ClusterCenter table!");
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("ClusterCenter created successfully!");
		
		//creating the Word2Vec table
		try {
			createWord2VecTable();
		} catch (SQLException e) {
			System.err.println("Could not create Word2Vec table!");
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Word2Vec created successfully!");
		
		//loading data to the Word2Vec table
		try {
			loadDataToWord2Vec();
		} catch (SQLException e) {
			System.err.println("Could not load data to the Word2Vec table!");
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("All data loaded successfully!");
		
		//disconnecting
		try {
			disconnect();
		} catch (SQLException e) {
			System.err.println("Could not disconnect from database!");
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("Disconnected succesfully!");
	}
	
	private static void disconnect() throws SQLException {
		connection.close();		
	}

	private static void loadDataToWord2Vec() throws SQLException {
		String sql = "SELECT partitionId FROM " + clusterCenterTableName;
		ResultSet res = statement.executeQuery(sql);
		ArrayList<String> partitionIDs = new ArrayList<String>();
		while (res.next()) {
			partitionIDs.add(res.getString(1));
		}		
		res.close();
		
		for (String partitionId : partitionIDs) {
			sql = "ALTER TABLE " + word2VecTableName + " " +
					   "ADD PARTITION(partitionId='" + partitionId + "') " +
					   "LOCATION '" + word2VecLocation + "/partitionId=" + partitionId + "'";

			statement.execute(sql);
		}
	}

	private static void createWord2VecTable() throws SQLException {
		String sql = "CREATE EXTERNAL TABLE " + word2VecTableName + "( " +
				"name String, " +
				"vector array<float>) " +
				"PARTITIONED BY ( " +
				"partitionId INT) " +
				"ROW FORMAT DELIMITED " +
				"FIELDS TERMINATED BY ':' " +
				"COLLECTION ITEMS TERMINATED BY ',' " +
				"STORED AS INPUTFORMAT " +
				"'org.apache.hadoop.mapred.TextInputFormat' " +
				"OUTPUTFORMAT " +
				"'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
				"LOCATION " +
				"'" + word2VecLocation + "'";
		statement.execute(sql);
	}

	private static void createClusterCenterTable() throws SQLException {
		String sql = "CREATE EXTERNAL TABLE " + clusterCenterTableName + "( " +
				"partitionId INT, " +
				"vector array<float>) " +
				"ROW FORMAT DELIMITED " +
				"FIELDS TERMINATED BY ':' " +
				"COLLECTION ITEMS TERMINATED BY ',' " +
				"STORED AS INPUTFORMAT " +
				"'org.apache.hadoop.mapred.TextInputFormat' " +
				"OUTPUTFORMAT " +
				"'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
				"LOCATION " +
				"'" + clusterCenterLocation + "' ";
		statement.execute(sql);
	}

	private static void dropTables() throws SQLException {
		String sql = "DROP TABLE IF EXISTS " + word2VecTableName;
		statement.execute(sql);
		sql = "DROP TABLE IF EXISTS " + clusterCenterTableName;
		statement.execute(sql);
	}

	private static void connect() throws ClassNotFoundException, SQLException{
		Class.forName(driverName);
		connection = DriverManager.getConnection(connectionURL);
	}
	
}
