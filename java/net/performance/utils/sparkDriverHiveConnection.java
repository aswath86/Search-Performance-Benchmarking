package net.performance.utils;

import java.sql.*;


public class sparkDriverHiveConnection {

	private static sparkDriverHiveConnection impalaIsntance;
	private static Connection con;

	private sparkDriverHiveConnection() {
		// private constructor //
	}

	public static sparkDriverHiveConnection getInstance() {
		if (impalaIsntance == null) {
			impalaIsntance = new sparkDriverHiveConnection();
		}
		return impalaIsntance;
	}

	public Connection getConnection() {
		if (con == null) {

			System.setProperty("hadoop.home.dir", "C:\\winutil\\");

			try {				
				
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				System.out.println("getting Spark Thrift connection");
				con = DriverManager
						.getConnection(
								"jdbc:hive2://localhost:4050/default");
				System.out.println("got connection");
				
			} catch (Exception ex) {
				ex.printStackTrace();

			}
		}
		return con;
	}
}
