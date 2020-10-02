package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lte;

import java.text.DateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

@Repository
public class TopLevelAnalyticsUtilOVandIVandnOV {
	@Autowired
	MongoTemplate mongoTemplate;
	
	@Autowired
	Common common;
	
	Logger logger = LoggerFactory.getLogger(getClass());

	
	public double getAllTotalValuesFromOrdersByFilterAndDates(Map<String, List<String>> filterMap, List<String> yyyymm, String type) {

		List<Bson> pipeLine = new ArrayList<>();

		setCommonDateFiltersForPOToLimitFutureDate(pipeLine);
		
		if(filterMap!=null) {
			Bson q1QryMatch = common.formMatchClauseForListFilterBson(filterMap);
			pipeLine.add(q1QryMatch);
		}
		
		
		String qConv = "{$project:{"
				+ "invoiceNumber:'$invoiceNumber', "
				+ "invoiceQuantity:'$invoiceQuantity', "
				+ "invoiceUnitPriceAsPerTc:'$invoiceUnitPriceAsPerTc', "
				+ "purchaseOrderNumber:'$purchaseOrderNumberOne', "
				+ "quantityOrderedPurchaseOrder: '$quantityOrderedPurchaseOrder', "
				+ "netPricePOPrice: '$netPricePOPrice', "
				+ "priceUnitPo: '$priceUnitPo', "
				+ "invoiceDateyymm : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
				+ "purchaseOrderCreationDateyymm : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }}"
		+ "} }";
		Bson q1QryConv = BasicDBObject.parse(qConv);
		pipeLine.add(q1QryConv);
		
		
		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			if(type.equalsIgnoreCase(Constants.INVOICE_VALUE)) {
				Bson dateFilter = match(in("invoiceDateyymm", yyyymm));
				pipeLine.add(dateFilter);
			}else {
				Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
				pipeLine.add(dateFilter);
			}
			
		}

		
		if(type.equalsIgnoreCase(Constants.INVOICE_VALUE)){
			String qCalc = "{ $project: { invoiceNumber:'$invoiceNumber', "
					+ "value: {$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']} } }";
			Bson q1QryCalc = BasicDBObject.parse(qCalc);
			pipeLine.add(q1QryCalc);
			
		}else if(type.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS)) {

			String projectPOIss = "{$group:{_id:'$purchaseOrderNumber' , "
					+ "val :{$sum:1} }}";
			Bson bPrjOIss = BasicDBObject.parse(projectPOIss);
			pipeLine.add(bPrjOIss);
			
		}else {
			String qCalc = "{ $project: { purchaseOrderNumber:'$purchaseOrderNumberOne', "
					+ "value:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']} } }";
			Bson q1QryCalc = BasicDBObject.parse(qCalc);
			pipeLine.add(q1QryCalc);
		}
		
		
		if(type.equalsIgnoreCase(Constants.INVOICE_VALUE)){
			String qSum = "{$group:{_id:null, total:{$sum:'$value'}}}";
			Bson q1QrySum = BasicDBObject.parse(qSum);
			pipeLine.add(q1QrySum);
		}else if(type.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS)) {
			String qSum = "{$group:{_id:null, total:{$sum:1}}}";
			Bson q1QrySum = BasicDBObject.parse(qSum);
			pipeLine.add(q1QrySum);
		}else {
			String qSum = "{$group:{_id:null, total:{$sum:'$value'}}}";
			Bson q1QrySum = BasicDBObject.parse(qSum);
			pipeLine.add(q1QrySum);
		}
		
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME).aggregate(
				pipeLine
			)
		;
		
		
		double totalOV = 0.0;
		try {
			if(type.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS))
				totalOV = ret.first().getInteger("total").doubleValue();
			else
				totalOV = ret.first().getDouble("total");
		}catch(NullPointerException nx) {
			logger.error(nx.getMessage());
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return totalOV;
	}
	
	
	public Map<String, Double> getAllValuesFromOrdersByFiltersAndDateRangeByDate(String startDate, 
			String endDate, Map<String, List<String>> filter, String type) {
		
		final String collectionName = Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME;
		List<Bson> pipeLine = new ArrayList<Bson>(); 
		
		Map<String, Double> retMap = new TreeMap<>();

		setCommonDateFiltersForPOToLimitFutureDate(pipeLine);
		
		if(filter!=null)
		pipeLine.add(common.formMatchClauseForListFilterBson(filter));
		
		if(type.equalsIgnoreCase(Constants.INVOICE_VALUE)) {
			List<Bson> dtFilters = new ArrayList<Bson>();
			dtFilters.add(Filters.gte("invoiceDate", LocalDate.parse(startDate, DateTimeFormatter.ISO_DATE)));
			dtFilters.add(Filters.lte("invoiceDate", LocalDate.parse(endDate, DateTimeFormatter.ISO_DATE)));
			Bson q1MatchQry = match(Filters.and(dtFilters));
			pipeLine.add(q1MatchQry);
		}else {

			List<Bson> dtFilters = new ArrayList<Bson>();
			dtFilters.add(Filters.gte("purchaseOrderCreationDate", LocalDate.parse(startDate, DateTimeFormatter.ISO_DATE)));
			dtFilters.add(Filters.lte("purchaseOrderCreationDate", LocalDate.parse(endDate, DateTimeFormatter.ISO_DATE)));
			Bson q1MatchQry = match(Filters.and(dtFilters));
			pipeLine.add(q1MatchQry);
//			printDocs(collectionName, pipeLine);
		}


		if(type.equalsIgnoreCase(Constants.INVOICE_VALUE)) {
			String qConv =  "{ $project: { gendate:{$dateToString: { format: '%Y-%m-%d', date: '$invoiceDate' }}, "
					+ "value: {$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']} } }";
			Bson q1QryConv = BasicDBObject.parse(qConv);
			pipeLine.add(q1QryConv);			
		
			String qagg =  "{ $group: { _id: '$gendate', "
					+ "value: {'$sum' : '$value'} } }";
			Bson q1QryAgg = BasicDBObject.parse(qagg);
			pipeLine.add(q1QryAgg);			
		
		}else if(type.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS)){
			String qConv = "{$group:{"
					+ "_id: {"
					+ "gendate:{$dateToString: { format: '%Y-%m-%d', date: '$purchaseOrderCreationDate' }}, "
					+ "purchaseOrderNumberOne : '$purchaseOrderNumberOne'}"
					+ "} }";
			Bson q1QryConv = BasicDBObject.parse(qConv);
			pipeLine.add(q1QryConv);

			
			String qSum = "{$group:{_id:'$_id.gendate', value:{$sum:1}}}";
			Bson q1QrySum = BasicDBObject.parse(qSum);
			pipeLine.add(q1QrySum);
		}else {
			String qConv = "{ $project: { gendate:{$dateToString: { format: '%Y-%m-%d', date: '$purchaseOrderCreationDate' }}, "
					+ "value:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']} } }";
			Bson q1QryConv = BasicDBObject.parse(qConv);
			pipeLine.add(q1QryConv);			
			String qagg =  "{ $group: { _id: '$gendate', "
					+ "value: {'$sum' : '$value'} } }";
			Bson q1QryAgg = BasicDBObject.parse(qagg);
			pipeLine.add(q1QryAgg);			
//			printDocs(collectionName, pipeLine);

		}

		
				
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collectionName)
				.aggregate(pipeLine);
		

		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
			if(type.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS))
				retMap.put(doc.getString("_id"), doc.getInteger("value").doubleValue());
			else
				retMap.put(doc.getString("_id"), doc.getDouble("value"));
		});
		
		return retMap;
	}


	
	public Map<String, Double> getAllValuesFromOrdersByFiltersForLast7Days(Map<String, List<String>> filter, String type) {
		Map<String, Double> retMap = new TreeMap<>();
		String endDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
		String startDate = LocalDate.now().minusDays(6).format(DateTimeFormatter.ISO_DATE);
		retMap = getAllValuesFromOrdersByFiltersAndDateRangeByDate(startDate, endDate, filter, type);
		return retMap;
	}

	
	
	private void setCommonDateFiltersForPOToLimitFutureDate(List<Bson> pipeline) {
		Bson purgeDates1 = match(lte("invoiceDate", LocalDate.now().plusYears(100)));
		Bson purgeDates2 = match(lte("purchaseOrderCreationDate", LocalDate.now().plusYears(100)));

		pipeline.add(purgeDates1);
		pipeline.add(purgeDates2);
	}

	private void printDocs(String collection, List<Bson> pipeline) {
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug(">>>>>>>>>>{}", doc);
		});

	}

	
}
