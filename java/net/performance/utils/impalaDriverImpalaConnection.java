package net.performance.utils;

import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class impalaDriverImpalaConnection {

	private static impalaDriverImpalaConnection impalaIsntance;
	private static Connection con;

	private impalaDriverImpalaConnection() {
		// private constructor //
	}

	public static impalaDriverImpalaConnection getInstance() {
		if (impalaIsntance == null) {
			impalaIsntance = new impalaDriverImpalaConnection();
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
				Class.forName("com.cloudera.impala.jdbc41.Driver");
				System.out.println("getting connection");
				con = DriverManager
						.getConnection("jdbc:impala://impala.dev.com:21050/intg_plt;AuthMech=1;KrbHostFQDN=impala.dev.com;principal=impala/impala.dev.com@NA.CORP.COM;KrbServiceName=impala");
				System.out.println("got connection");
			} catch (Exception ex) {
				ex.printStackTrace();

			}
		}
		return con;
	}
}
