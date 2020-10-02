package com.vf.ana;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.eq;

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
import org.springframework.stereotype.Repository;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Filters;

@Repository
public class TopLevelAnalyticsUtilPAandActiveItems {

	@Autowired
	MongoTemplate mongoTemplate;
	
	@Autowired
	Common common;
	
	Logger logger = LoggerFactory.getLogger(getClass());

	public int getTotalActivePAsORActiveItemsByDate(String date1, Map<String, List<String>> filter, boolean isPA) {

		List<Integer> retList = new ArrayList<Integer>();

		
		int size = 3;
		if(date1==null) size=1;
		
		if(filter!=null) size = size+filter.size();
		
		Bson[] filters = new Bson[size];
		filters[0] = Filters.eq("priceAgreementStatus", "Active");
		
		if(date1!=null) {
			filters[1] = Filters.lte("validFromDate", LocalDate.parse(date1, DateTimeFormatter.ISO_DATE));
			filters[2] = Filters.gte("validToDate", LocalDate.parse(date1, DateTimeFormatter.ISO_DATE));
		}
		int cnt=3;
		if(date1==null) size=1;
		if(filter!=null) {
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
		if(isPA)
		multiIdMap1.put("opco", "$opcoCode");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Bson q3Qry = group(null, Accumulators.sum("count", 1));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME)
				.aggregate(Arrays.asList(q1Qry, q2Qry, q3Qry));

		try {
				ret5.cursor().forEachRemaining(doc -> {
				retList.add(doc.getInteger("count"));
			});
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		if(retList.size()==0) return 0; 
		else return retList.get(0);

	}
	

	public int getTotalActivePAsORActiveItemsByFilterAndAlsoDateFilter(Map<String, List<String>> filter, List<String> yyyymm, boolean isPA) {

		List<Integer> retList = new ArrayList<Integer>();

		List<Bson> pipeLine = new ArrayList<>();
		
		Bson activeFilter = match(eq("priceAgreementStatus", "Active"));
	
		pipeLine.add(activeFilter);
		
		
		if(filter != null) {
			pipeLine.add(common.formMatchClauseForListFilterBson(filter));
		}
		
		if(yyyymm!=null) {
			pipeLine.add(common.getYYYYMMFilterForPAandAI(yyyymm));
		}

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("spn", "$supplierPartNumber");
		if(isPA)
		multiIdMap1.put("opco", "$opcoCode");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));
		pipeLine.add(q2Qry);
		
		Bson q3Qry = group(null, Accumulators.sum("count", 1));
		pipeLine.add(q3Qry);

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME)
				.aggregate(pipeLine);

		try {
				ret5.cursor().forEachRemaining(doc -> {
				retList.add(doc.getInteger("count"));
			});
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		if(retList.size()==0) return 0; 
		else return retList.get(0);

	}
	
	
	
	
	public int getAllActivePAsOrActiveItems(boolean isPA) {

		return getTotalActivePAsORActiveItemsByDate(null, null, isPA);
	}

	
	
	
	
	public Map<String, Integer> getActivePAsOrActiveItemsWithFilterForLast7Days(Map<String, List<String>> filter, boolean isPA) {

		Map<String, Integer> retMap = new LinkedHashMap<>();
		LocalDate endDate = LocalDate.now();
		LocalDate startDate = LocalDate.now().minusDays(6);
		while(startDate.compareTo(endDate.plusDays(1))<0) {
			String dt = startDate.format(DateTimeFormatter.ISO_DATE);
			int num = getTotalActivePAsORActiveItemsByDate(dt, filter, isPA);
			retMap.put(dt, num);
			startDate= startDate.plusDays(1);
		}
		return retMap;
	}

	
	public Map<String, Integer> getActivePAsOrActiveItemsByFilterAndDateRange(String startDt, String endDt, Map<String, List<String>> filter, boolean isPA) {
		Map<String, Integer> retMap = new LinkedHashMap<>();
		LocalDate endDate = LocalDate.parse(endDt, DateTimeFormatter.ISO_DATE);
		LocalDate startDate = LocalDate.parse(startDt, DateTimeFormatter.ISO_DATE);;
		while(startDate.compareTo(endDate.plusDays(1))<0) {
			String dt = startDate.format(DateTimeFormatter.ISO_DATE);
			int num = getTotalActivePAsORActiveItemsByDate(dt, filter, isPA);
			retMap.put(dt, num);
			startDate= startDate.plusDays(1);
		}
		return retMap;
	}


}
