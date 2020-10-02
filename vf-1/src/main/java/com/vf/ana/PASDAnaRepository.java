package com.vf.ana;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

@Repository
public class PASDAnaRepository {

	@Autowired
	MongoTemplate mongoTemplate;

	Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * Given count of all active OLAS for the date passed as ISO String from
	 * priceAgreementSpnDetails
	 * 
	 * @param date1
	 * @return
	 */
	public int getActiveOLAsByDate(String date1) {

		List<Integer> retList = new ArrayList<Integer>();
		logger.debug("Date = " + LocalDate.parse(date1, DateTimeFormatter.ISO_DATE));

		Bson q1Qry = match(Filters.and(Filters.eq("priceAgreementStatus", "Active"),
				Filters.lte("validFromDate", LocalDate.parse(date1, DateTimeFormatter.ISO_DATE)),
				Filters.gte("validToDate", LocalDate.parse(date1, DateTimeFormatter.ISO_DATE))));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("ola", "$outlineAgreementNumber");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Bson q3Qry = group(null, Accumulators.sum("count", 1));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection(Constants.COMBINED_SPN_DETAILS_COLLECTION_NAME)
				.aggregate(Arrays.asList(
						q1Qry, 
						q2Qry 
						,q3Qry
						));

		ret5.cursor().forEachRemaining(doc -> {
			logger.debug("{}", doc);
			retList.add(doc.getInteger("count"));
		});

		return retList.get(0);

	}

	public Map<String, Integer> getActiveOLAsByDateRange(String date1, String date2, Map<String, String> filter) {
		
		
		LocalDate dtStart = LocalDate.parse(date1, DateTimeFormatter.ISO_DATE);
		LocalDate dtEnd = LocalDate.parse(date2, DateTimeFormatter.ISO_DATE);
		
		int size = 2;
		if(filter!=null) size = size + filter.size();
		
		
		Map<String, Integer> retMap = new LinkedHashMap<>();
		
		while(dtStart.compareTo(dtEnd)<=0) {
			
			Query query = new Query();
			
			
			Criteria[] cArr = new Criteria[size];
			cArr[0]= Criteria.where("validToDate").gte(dtStart);
			cArr[1]=Criteria.where("validFromDate").lte(dtStart);
			
			if(filter!=null) {
				int cnt = 2;
				for(String key : filter.keySet()) {
					cArr[cnt++] = Criteria.where(key).is(filter.get(key));
				}
			}
			
			query.addCriteria(Criteria.where("priceAgreementStatus").is("Active").andOperator(
					cArr
			));
			
			int i =  mongoTemplate.findDistinct(query, "outlineAgreementNumber", "priceAgreementSpnDetails", 
					String.class).size();
			
			retMap.put(dtStart.format(DateTimeFormatter.ISO_DATE), i);
			dtStart = dtStart.plusDays(1);

		}
		
		
		return retMap;
		
		
	}

	public Map<String, Integer> getActivePAsByFilterAndDateRange(String date1, String date2, Map<String, String>filter) {
		
		LocalDate dtStart = LocalDate.parse(date1, DateTimeFormatter.ISO_DATE);
		LocalDate dtEnd = LocalDate.parse(date2, DateTimeFormatter.ISO_DATE);
		
		
		Map<String, Integer> retMap = new LinkedHashMap<>();
		
		while(dtStart.compareTo(dtEnd)<=0) {
			String yr = dtStart.getYear()+"";
			String mn = dtStart.getMonthValue()+"";
			mn = mn.length()==1?"0"+mn:mn;
			
			String dy = dtStart.getDayOfMonth()+"";
			dy = dy.length()==1?"0"+dy:dy;
			
			String dtStr = yr+"-"+mn+"-"+dy;
			int num = getActivePAsORActiveItemsByDate(dtStr, filter);
			retMap.put(dtStr, num);
			dtStart = dtStart.plusDays(1);
		}
		
		return retMap;
		
		
	}
	/**
	 * Gets count of all active OLAs from priceAgreementSpnDetails
	 * 
	 * @return
	 */
	public int getNumberOfALLActiveOLAs() {

		List<Integer> retList = new ArrayList<Integer>();

		Bson q1Qry = match(Filters.and(Filters.eq("priceAgreementStatus", "Active")));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("ola", "$outlineAgreementNumber");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Bson q3Qry = group(null, Accumulators.sum("count", 1));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection("priceAgreementSpnDetails")
				.aggregate(Arrays.asList(q1Qry
						, q2Qry
						, q3Qry
						));

		ret5.cursor().forEachRemaining(doc -> {
			retList.add(doc.getInteger("count"));
		});

		return retList.get(0);

	}

	/**
	 * Gets count of all active OLAs from priceAgreementSpnDetails
	 * 
	 * @return
	 */
	public int getNumberOfALLActivePAs() {

		List<Integer> retList = new ArrayList<Integer>();

		Bson q1Qry = match(Filters.and(Filters.eq("priceAgreementStatus", "Active")));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("spn", "$supplierPartNumber");
		multiIdMap1.put("opco", "$opcoCode");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Bson q3Qry = group(null, Accumulators.sum("count", 1));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection("priceAgreementSpnDetails")
				.aggregate(Arrays.asList(q1Qry
						, q2Qry
						, q3Qry
						));

		ret5.cursor().forEachRemaining(doc -> {
			logger.debug("{}", doc);
			retList.add(doc.getInteger("count"));
		});

		return retList.get(0);

	}
	

	@Autowired
	Common common;
	
	private int getNumberOfALLActivePAsByFilter(Map<String, String> filterMap, String date) {


		String filterMatchQry = common.formMatchClauseForFilter(filterMap, null, null);
		Bson q1filterMatch = BasicDBObject.parse(filterMatchQry);
		
		
		Bson q1QryDate = null;
		if(date!=null) {
			q1QryDate = match(Filters.and(Filters.eq("priceAgreementStatus", "Active"),
					Filters.lte("validFromDate", LocalDate.parse(date, DateTimeFormatter.ISO_DATE)),
					Filters.gte("validToDate", LocalDate.parse(date, DateTimeFormatter.ISO_DATE))));
		}
		
		
		Bson q1Qry = match(Filters.and(Filters.eq("priceAgreementStatus", "Active")));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("spn", "$supplierPartNumber");
		multiIdMap1.put("opco", "$opcoCode");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Bson q3Qry = group(null, Accumulators.sum("count", 1));

		List<Bson> pipeLine = new ArrayList<Bson>(); 
		
		pipeLine.add(q1filterMatch);
		if(q1QryDate!=null)
		pipeLine.add(q1QryDate);
		pipeLine.add(q1Qry);
		pipeLine.add(q2Qry);
		pipeLine.add(q3Qry);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("priceAgreementSpnDetails")
				.aggregate(pipeLine);

//		ret.cursor().forEachRemaining(doc->{
//			logger.debug(">>>>{}",doc);
//		});
		
		int totalOV = 0;
		try {
			totalOV = ret.first().getInteger("count");
		}catch(NullPointerException ex) {
			
		}
		
		return totalOV;


	}
	
	
	
	
	public int getActivePAsByDate(String date1, Map<String, List<String>> filter) {

		List<Integer> retList = new ArrayList<Integer>();
//		logger.debug(">>>>>Date = " + date1);
//		logger.debug(">>>>>Date = " + LocalDate.parse(date1, DateTimeFormatter.ISO_DATE));

		
		int size = 3;
		if(filter!=null) size = size+filter.size();
		
		Bson[] filters = new Bson[size];
		filters[0] = Filters.eq("priceAgreementStatus", "Active");
		filters[1] = Filters.lte("validFromDate", LocalDate.parse(date1, DateTimeFormatter.ISO_DATE));
		filters[2] = Filters.gte("validToDate", LocalDate.parse(date1, DateTimeFormatter.ISO_DATE));
		if(filter!=null) {
			int cnt=3;
			for(String key : filter.keySet()) {
				filters[cnt] = Filters.in(key, filter.get(key));
				cnt++;
			}
		}
		
		Bson q1Qry = match(Filters.and(
				filters
				));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("spn", "$supplierPartNumber");
		multiIdMap1.put("opco", "$opcoCode");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Bson q3Qry = group(null, Accumulators.sum("count", 1));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME)
				.aggregate(Arrays.asList(q1Qry, q2Qry, q3Qry));

		ret5.cursor().forEachRemaining(doc -> {
//			logger.debug("{}", doc);
			retList.add(doc.getInteger("count"));
		});

		return retList.get(0);

	}


	/**
	 * Gets count of all active OLAS grouped by property from
	 * priceAgreementSpnDetails. The property name has to be passed based on
	 * 
	 * @param dimName
	 * @return
	 */
	public Map<String, Integer> getALLActiveOLAsByDim(String dimName) {

		Map<String, Integer> retmap = new HashMap<String, Integer>();

		Bson q1Qry = match(Filters.and(Filters.eq("priceAgreementStatus", "Active")));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("ola", "$outlineAgreementNumber");
		multiIdMap1.put("mcat", "$" + dimName);

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Map<String, Object> multiIdMap2 = new HashMap<String, Object>();
		multiIdMap2.put("mcat", "$_id.mcat");
		Document groupFields3 = new Document(multiIdMap2);
		Bson q3Qry = group(groupFields3, Accumulators.sum("count", 1));

		Bson q4Qry = sort(Sorts.ascending("count"));
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection("priceAgreementSpnDetails")
				.aggregate(Arrays.asList(q1Qry, q2Qry, q3Qry, q4Qry));

		ret5.cursor().forEachRemaining(doc -> {
//			logger.debug(doc);
			retmap.put(((Document) doc.get("_id")).getString("mcat"), doc.getInteger("count"));

		});

		logger.debug("{}", retmap);
		return retmap;
	}

	/**
	 * Gets count of all active PAs grouped by property from
	 * priceAgreementSpnDetails. The property name has to be passed based on
	 * 
	 * @param dimName
	 * @return
	 */
	public Map<String, Integer> getALLActivePAsByDim(String dimName) {

		Map<String, Integer> retmap = new HashMap<String, Integer>();

		Bson q1Qry = match(Filters.and(Filters.eq("priceAgreementStatus", "Active")));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("spn", "$supplierPartNumber");
		multiIdMap1.put("opco", "$opcoCode");
		multiIdMap1.put("mcat", "$" + dimName);

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Map<String, Object> multiIdMap2 = new HashMap<String, Object>();
		multiIdMap2.put("mcat", "$_id.mcat");
		Document groupFields3 = new Document(multiIdMap2);
		Bson q3Qry = group(groupFields3, Accumulators.sum("count", 1));

		Bson q4Qry = sort(Sorts.ascending("count"));
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection(Constants.COMBINED_SPN_DETAILS_COLLECTION_NAME)
				.aggregate(Arrays.asList(q1Qry, q2Qry
//						, q3Qry
//						, q4Qry
						));

		ret5.cursor().forEachRemaining(doc -> {
//			logger.debug("{}",doc);
			retmap.put(((Document) doc.get("_id")).getString("mcat"), doc.getInteger("count"));

		});

		logger.debug("{}", retmap);
		return retmap;
	}
	
	/**
	 * Gets count of all active Items from priceAgreementSpnDetails
	 * 
	 * @return
	 */
	public int getNumberOfALLActiveItems() {

		List<Integer> retList = new ArrayList<Integer>();

		Bson q1Qry = match(Filters.and(Filters.eq("priceAgreementStatus", "Active")));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("supplierPartNumber", "$supplierPartNumber");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Bson q3Qry = group(null, Accumulators.sum("count", 1));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection("priceAgreementSpnDetails")
				.aggregate(Arrays.asList(q1Qry
						, q2Qry
						, q3Qry
						));

		ret5.cursor().forEachRemaining(doc -> {
			retList.add(doc.getInteger("count"));
		});

		return retList.get(0);

	}
	

	public int getALLActiveItemsByDimension(String dimName) {

		List<Integer> retList = new ArrayList<Integer>();

		Bson q1Qry = match(Filters.and(Filters.eq("priceAgreementStatus", "Active")));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("supplierPartNumber", "$supplierPartNumber");
		multiIdMap1.put("dim", "$"+dimName);

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Bson q3Qry = group(null, Accumulators.sum("count", 1));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection("priceAgreementSpnDetails")
				.aggregate(Arrays.asList(q1Qry
						, q2Qry
						, q3Qry
						));

		ret5.cursor().forEachRemaining(doc -> {
			retList.add(doc.getInteger("count"));
			logger.debug("{}", doc);
		});

		return retList.get(0);

	}
	
	
	
	
	
	public Map<String, Integer> getActiveItemsByFilterAndDateRange(String date1, String date2, Map<String, String>filter) {
		
		LocalDate dtStart = LocalDate.parse(date1, DateTimeFormatter.ISO_DATE);
		LocalDate dtEnd = LocalDate.parse(date2, DateTimeFormatter.ISO_DATE);
		
		
		Map<String, Integer> retMap = new LinkedHashMap<>();
		
		while(dtStart.compareTo(dtEnd)<=0) {
			String yr = dtStart.getYear()+"";
			String mn = dtStart.getMonthValue()+"";
			mn = mn.length()==1?"0"+mn:mn;
			
			String dy = dtStart.getDayOfMonth()+"";
			dy = dy.length()==1?"0"+dy:dy;
			
			String dtStr = yr+"-"+mn+"-"+dy;
			int num = getNumberOfALLActiveItemsByFilter(filter, dtStr);
			retMap.put(dtStr, num);
			dtStart = dtStart.plusDays(1);
		}
		
		return retMap;
		
		
	}

	
	
	/**
	 * Gets count of all active Items from priceAgreementSpnDetails BY filter and date
	 * 
	 * @return
	 */
	public int getNumberOfALLActiveItemsByFilter(Map<String, String> filterMap, String date) {

		List<Bson> pipeLine = new ArrayList<Bson>(); 

		if(filterMap!=null) {
			String filterMatchQry = common.formMatchClauseForFilter(filterMap, null, null);
		
			Bson q1filterMatch = BasicDBObject.parse(filterMatchQry);

			pipeLine.add(q1filterMatch);
		}

		
		
		Bson q1QryDate = null;
		if(date!=null) {
			q1QryDate = match(Filters.and(Filters.eq("priceAgreementStatus", "Active"),
					Filters.lte("validFromDate", LocalDate.parse(date, DateTimeFormatter.ISO_DATE)),
					Filters.gte("validToDate", LocalDate.parse(date, DateTimeFormatter.ISO_DATE))));
		}

		Bson q1Qry = match(Filters.and(Filters.eq("priceAgreementStatus", "Active")));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("supplierPartNumber", "$supplierPartNumber");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Bson q3Qry = group(null, Accumulators.sum("count", 1));

		if(q1QryDate!=null)
		pipeLine.add(q1QryDate);
		pipeLine.add(q1Qry);
		pipeLine.add(q2Qry);
		pipeLine.add(q3Qry);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("priceAgreementSpnDetails")
				.aggregate(pipeLine);

//		ret.cursor().forEachRemaining(doc->{
//			logger.debug(">>>>{}",doc);
//		});
		
		int ai = 0;
		try {
			ai = ret.first().getInteger("count");
		}catch(NullPointerException ex) {
			
		}
		
		return ai;



	}
	
}
