package net.performance.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import net.performance.beans.Impala;
import net.performance.beans.Solr;
import net.performance.service.impalaDriverHive;
import net.performance.service.impalaDriverImpala;
import net.performance.service.solrService;
import net.performance.service.sparkDriverHive;

@Controller
public class PerformanceController {

	private static final Logger logger = LoggerFactory
			.getLogger(PerformanceController.class);

	@RequestMapping(value = "/impala/hive-driver", method = RequestMethod.GET)
	@ResponseBody
	public Impala getImpalaCountHiveDriver(
			@RequestParam(value = "query", required = false) String impalaCorrelationQuery)
			throws Exception {
		logger.info("Start getImpalaCountHiveDriver");
		return impalaDriverHive.getImpalaCount(impalaCorrelationQuery);
	}
	
	
	@RequestMapping(value = "/impala/impala-driver", method = RequestMethod.GET)
	@ResponseBody
	public Impala getImpalaCountImpalaDriver(
			@RequestParam(value = "query", required = false) String impalaCorrelationQuery)
			throws Exception {
		logger.info("Start getImpalaCountImpalaDriver");
		return impalaDriverImpala.getImpalaCount(impalaCorrelationQuery);
	}
	
	@RequestMapping(value = "/spark/hive-driver", method = RequestMethod.GET)
	@ResponseBody
	public Impala getSparkCountHiveDriver(
			@RequestParam(value = "query", required = false) String impalaCorrelationQuery)
			throws Exception {
		logger.info("Start getSparkCountHiveDriver");
		return sparkDriverHive.getImpalaCount(impalaCorrelationQuery);
	}
	
	@RequestMapping(value = "/solr", method = RequestMethod.GET)
	@ResponseBody
	public Solr getSolrCount(
			@RequestParam(value = "query", required = false) String solrCorrelationQuery,
			@RequestParam(value = "collection", required = false) String solrCollection,
			@RequestParam(value = "solrHost", required = false) String solrHost,
			@RequestParam(value = "count", required = false) String count)
			throws Exception {
		//logger.info("Start getSolrCount");
		return new solrService().getSolrCount(solrCorrelationQuery, solrCollection, count, solrHost);
	}

}
