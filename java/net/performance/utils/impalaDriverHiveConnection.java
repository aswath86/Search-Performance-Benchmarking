package net.performance.utils;

import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.performance.controller.PerformanceController;

public class impalaDriverHiveConnection {

	private static impalaDriverHiveConnection impalaIsntance;
	private static Connection con;

	private static final Logger logger = LoggerFactory
			.getLogger(impalaDriverHiveConnection.class);
	
	private impalaDriverHiveConnection() {
		// private constructor //
	}

	public static impalaDriverHiveConnection getInstance() {
		if (impalaIsntance == null) {
			impalaIsntance = new impalaDriverHiveConnection();
		}
		return impalaIsntance;
	}

	public Connection getConnection() {
		if (con == null) {

			System.setProperty("hadoop.home.dir", "C:\\winutil\\");

			try {
				System.setProperty("sun.security.krb5.debug", "false");
				Configuration conf = new Configuration();
				conf.set("hadoop.security.authentication", "Kerberos");
				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation
						.loginUserFromKeytab(
								"s-tbdpegrd-n",
								"C:\\Users\\Aswath\\Downloads\\scala-SDK-4.5.0-vfinal-2.11-win32.win32.x86_64\\HIVE-JDBC\\s-egrd-n.keytab");
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				System.out.println("getting connection");
				logger.info("getting connection");
				con = DriverManager
						.getConnection("jdbc:hive2://impala.dev.com:21050/intg_plt;principal=impala/impala.dev.com@NA.CORP.COM");
				logger.info("got connection");
			} catch (Exception ex) {
				ex.printStackTrace();				
			}
		}
		return con;
	}
}
