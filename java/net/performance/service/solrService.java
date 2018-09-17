package net.performance.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import org.json.*;
import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.performance.beans.Solr;

public class solrService {
	
	private static final Logger logger = LoggerFactory
			.getLogger(solrService.class);

	private static final Logger loggerQuery = LoggerFactory
			.getLogger("PerformanceBenchMarking");
	
	static HashMap<String, String> solrmap = null;
	
	static {
		solrmap = new HashMap<>();
	}
	
	public Solr getSolrCount(String solrCorrelationQuery, String solrCollection, String queryIndex, String solrHost)
			throws IOException {				

		if(solrmap.containsKey(solrCorrelationQuery)) {
			return getSolrCountFromHashMap(solrCorrelationQuery, solrCollection, queryIndex, solrHost);
		} else {
			return getSolrCountFromSOLR(solrCorrelationQuery, solrCollection, queryIndex, solrHost);
		}
	} 
	private Solr getSolrCountFromHashMap(String solrCorrelationQuery, String solrCollection, String queryIndex, String solrHost)
			throws IOException {				

		
		long entryTime = System.currentTimeMillis();
		logger.info("<<<here>>> Enter hashmap Solr Thread <<<>>> count: "+ queryIndex+ " - " + entryTime);
		Solr countOfSolrQuery = new Solr();	
		try{
			
		long startTime = System.currentTimeMillis();
		String jsonText = solrmap.get(solrCorrelationQuery);
		long endTime = System.currentTimeMillis();
		
		logger.info("count :" + queryIndex + " <<< " + startTime+ " - " + endTime + " >>> Time Taken ---> " + (endTime - startTime)
				/ 1000 + " <<<>>> " );
		
		countOfSolrQuery.setCountOfImpalaQuery(jsonText);
		
		int countOfTopRecords = 0;
		double qTime = 0;
		
			JSONObject obj = new JSONObject(jsonText);
			countOfTopRecords = Integer.parseInt(obj.getJSONObject("response").getString("numFound"));
			qTime = Integer.parseInt(obj.getJSONObject("responseHeader").getString("QTime"));
		
		
		loggerQuery.warn(queryIndex + "|" +  countOfTopRecords + "|" + new Date(startTime)+"|"+ new Date(endTime)+"|"+(endTime - startTime)/ 1000.0 + "|" + qTime/1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return countOfSolrQuery;
	} 
	
	private Solr getSolrCountFromSOLR(String solrCorrelationQuery, String solrCollection, String queryIndex, String solrHost)
			throws IOException {				

		
		long entryTime = System.currentTimeMillis();
		logger.info("<<<here>>> Enter Solr Thread <<<>>> count: "+ queryIndex+ " - " + entryTime);
		Solr countOfSolrQuery = new Solr();	
		try{
		String solrQuery = "http://"+solrHost+"/solr/"+ solrCollection +"/select?indent=on&wt=json&q=*:*&rows=0&" + URLEncoder.encode(solrCorrelationQuery,"UTF-8");
		//String solrQuery = "http://q360solr.dev.toyota.com/solr/pqss_combined_32_4R/select?indent=on&wt=json&q=*:*&rows=10&fq=" + URLEncoder.encode(solrCorrelationQuery,"UTF-8");
		
		String solrQueryDecoded = solrQuery.replaceAll("%3D", "=").replaceAll("%26", "&");		
			
		long startTime = System.currentTimeMillis();
		BufferedReader rd = new BufferedReader(
				new InputStreamReader(((HttpURLConnection) (new URL(solrQueryDecoded))
						.openConnection()).getInputStream()));		
		String jsonText = readAll(rd);
		long endTime = System.currentTimeMillis();
		System.out.println("Time Taken ---> " + (endTime - startTime)
				/ 1000 + " <<<>>> " + URLDecoder.decode(solrQueryDecoded, "UTF-8"));
		logger.info("count :" + queryIndex + " <<< " + startTime+ " - " + endTime + " >>> Time Taken ---> " + (endTime - startTime)
				/ 1000 + " <<<>>> " + URLDecoder.decode(solrQueryDecoded, "UTF-8"));
		
		countOfSolrQuery.setCountOfImpalaQuery(jsonText);
		
		int countOfTopRecords = 0;
		double qTime = 0;
		
			JSONObject obj = new JSONObject(jsonText);
			countOfTopRecords = Integer.parseInt(obj.getJSONObject("response").getString("numFound"));
			qTime = Integer.parseInt(obj.getJSONObject("responseHeader").getString("QTime"));
		
		solrmap.put(solrCorrelationQuery, jsonText);
		loggerQuery.warn(queryIndex + "|" +  countOfTopRecords + "|" + new Date(startTime)+"|"+ new Date(endTime)+"|"+(endTime - startTime)/ 1000.0 + "|" + qTime/1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return countOfSolrQuery;
	} 

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

}
