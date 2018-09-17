package net.performance.service;

import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.performance.beans.Impala;
import net.performance.controller.PerformanceController;
import net.performance.utils.impalaDriverHiveConnection;

public class impalaDriverHive {
	
	private static final Logger logger = LoggerFactory
			.getLogger(impalaDriverHive.class);
	
	public static Impala getImpalaCount(String impalaCorrelationQuery) {
		System.out.println("<<<>>> Enter impalaDriverHive Thread <<<>>> " + System.currentTimeMillis());
		logger.info("<<<>>> Enter impalaDriverHive Thread <<<>>> " + System.currentTimeMillis());

		StringBuffer sb = new StringBuffer();
		Impala impalaCountQuery = new Impala();
		try {
			Connection con = impalaDriverHiveConnection.getInstance().getConnection();
			Statement stmt = con.createStatement();
			long startTime = System.currentTimeMillis();
			ResultSet res = stmt.executeQuery(impalaCorrelationQuery);
			while (res.next()) {
				System.out.println(res.getString(1));
				logger.info(res.getString(1));
				sb.append(res.getString(1));
				break;
			}
			long endTime = System.currentTimeMillis();
			System.out.println("Time Taken ---> " + (endTime - startTime)
					/ 1000 + " <<<>>> " + impalaCorrelationQuery);
			logger.info("Time Taken ---> " + (endTime - startTime)
					/ 1000 + " <<<>>> " + impalaCorrelationQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
		impalaCountQuery.setCountOfImpalaQuery(sb.toString());
		return impalaCountQuery;
	}
}
