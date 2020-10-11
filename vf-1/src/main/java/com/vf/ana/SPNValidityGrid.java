package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.regex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

@Service
public class SPNValidityGrid {
	
	@Autowired
	MongoTemplate mongoTemplate;

	
	@Autowired
	Common common;
	
	public List<Map<String, String>> getSPNValidityGrid(int days, int pgSZ, int pgNo, Map<String, String> filterMap, String orderByField, int orderByDir){
		
		List<Map<String, String>> retList = new ArrayList<>();

		String collectionName = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		List<Bson> pipeline = new ArrayList<>();

		long factorDay2Milli = 1000*60*60*24;
		
		Logger logger = LoggerFactory.getLogger(getClass());

		String bson1 = " {$project:{ "
				+ "_id: 0,"
				+ "supplierPartNumber:1,"
				+ "materialGroupL4:1,"
				+ "materialShortDesc:1,"
				+ "outlineAgreementNumber:1,"
				+ "priceAgreementReferenceName:1,"
				+ "catalogueType:1,"
				+ "supplierName:1,"
				+ "validToDate:1, "
				+ "diff: {'$subtract':['$validToDate', new Date()]},"
				+ " }},";
		
		pipeline.add(BasicDBObject.parse(bson1));
		
		
		if(filterMap !=null && filterMap.size()>0) {
			List<Bson> andList = new ArrayList<>();
			for(String key : filterMap.keySet()) {
				String val = filterMap.get(key);
				andList.add(regex(key, val, "i"));
			}
			if(andList.size()>0) {
				pipeline.add(match(Filters.and(andList)));
			}
		}
		
		long millisec = days*factorDay2Milli;
		pipeline.add(match(Filters.lte("diff", millisec)));
		
		if(orderByField==null || orderByField.trim().length()==0) {
			orderByField="diff";
		}
		if(orderByDir==Constants.SORT_DIRECTION_DESCENDING) {
			pipeline.add(Aggregates.sort(Sorts.descending(orderByField)));
		}else {
			pipeline.add(Aggregates.sort(Sorts.ascending(orderByField)));
		}
		
		
		pipeline.add(Aggregates.skip(pgSZ*pgNo));
		pipeline.add(Aggregates.limit(pgSZ));
				
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> rec = mongo.getCollection(collectionName).aggregate(
				pipeline
			)
		;
		rec.cursor().forEachRemaining(doc->{
			Map<String, String> tmp = new HashMap<>();
			tmp.put("supplierPartNumber", doc.getString("supplierPartNumber"));
			tmp.put("materialGroupL4", doc.getString("materialGroupL4"));
			tmp.put("materialShortDesc", doc.getString("materialShortDesc"));
			tmp.put("outlineAgreementNumber", doc.getString("outlineAgreementNumber"));
			tmp.put("priceAgreementReferenceName", doc.getString("priceAgreementReferenceName"));
			tmp.put("catalogueType", doc.getString("catalogueType"));
			tmp.put("supplierName", doc.getString("supplierName"));
			long timeInMillis=doc.getLong("diff");
			int day = (int) (timeInMillis/factorDay2Milli);
			tmp.put("day", day+"");
			retList.add(tmp);
		});
		logger.debug("{}", retList);
		logger.debug("{}", retList.size());
		return retList;

	}
	
	public int getSPNValidityGridCOUNT(int days, Map<String, String> filterMap){
		
		String collectionName = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		List<Bson> pipeline = new ArrayList<>();

		long factorDay2Milli = 1000*60*60*24;
		
		Logger logger = LoggerFactory.getLogger(getClass());

		String bson1 = " {$project:{ "
				+ "_id: 0,"
				+ "supplierPartNumber:1,"
				+ "materialGroupL4:1,"
				+ "materialShortDesc:1,"
				+ "outlineAgreementNumber:1,"
				+ "priceAgreementReferenceName:1,"
				+ "catalogueType:1,"
				+ "supplierName:1,"
				+ "validToDate:1, "
				+ "diff: {'$subtract':['$validToDate', new Date()]},"
				+ " }},";
		
		pipeline.add(BasicDBObject.parse(bson1));
		
		
		if(filterMap !=null && filterMap.size()>0) {
			List<Bson> andList = new ArrayList<>();
			for(String key : filterMap.keySet()) {
				String val = filterMap.get(key);
				andList.add(regex(key, val, "i"));
			}
			if(andList.size()>0) {
				pipeline.add(match(Filters.and(andList)));
			}
		}
		
		long millisec = days*factorDay2Milli;
		pipeline.add(match(Filters.lte("diff", millisec)));
		
//		if(orderByField==null || orderByField.trim().length()==0) {
//			orderByField="diff";
//		}
//		if(orderByDir==Constants.SORT_DIRECTION_DESCENDING) {
//			pipeline.add(Aggregates.sort(Sorts.descending(orderByField)));
//		}else {
//			pipeline.add(Aggregates.sort(Sorts.ascending(orderByField)));
//		}
		
		
//		pipeline.add(Aggregates.skip(pgSZ*pgNo));
//		pipeline.add(Aggregates.limit(pgSZ));
				
		int count = 0;
		count = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);
		return count;

	}

	
}
