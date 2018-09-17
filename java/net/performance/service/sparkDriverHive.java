package net.performance.service;

import java.sql.*;

import net.performance.beans.Impala;
import net.performance.utils.sparkDriverHiveConnection;

public class sparkDriverHive {
	public static Impala getImpalaCount(String impalaCorrelationQuery) {
		System.out.println("<<<>>> Enter impalaDriverHive Thread <<<>>> " + System.currentTimeMillis());

		StringBuffer sb = new StringBuffer();
		Impala impalaCountQuery = new Impala();
		try {
			Connection con = sparkDriverHiveConnection.getInstance().getConnection();
			Statement stmt = con.createStatement();
			long startTime = System.currentTimeMillis();
			ResultSet res = stmt.executeQuery(impalaCorrelationQuery);
			while (res.next()) {
				System.out.println(res.getString(1));
				sb.append(res.getString(1));
				break;
			}
			long endTime = System.currentTimeMillis();
			System.out.println("Time Taken ---> " + (endTime - startTime)
					/ 1000 + " <<<>>> " + impalaCorrelationQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
		impalaCountQuery.setCountOfImpalaQuery(sb.toString());

		// System.out.println(sb.toString());
		return impalaCountQuery;
	}
}
