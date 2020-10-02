package com.vf.ana;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

@Repository
public class PriceAgreementSpnDetailsRepository {
	@Autowired
	MongoTemplate mongoTemplate;
	
	@Autowired
	Common common;
	
	Logger logger = LoggerFactory.getLogger(getClass());


	public double getAllOrderValueIssued() {

//		String qConv = "{$project:{purchaseOrderNumber:'$purchaseOrderNumberOne', "
//				+ "quantityOrderedPurchaseOrder: {$toDouble:'$quantityOrderedPurchaseOrder'}, "
//				+ "netPricePOPrice: {$toDouble:'$netPricePOPrice'}, "
//				+ "priceUnitPo: {$toDouble:'$priceUnitPo'}, } }";

		String qConv = "{$project:{purchaseOrderNumber:'$purchaseOrderNumberOne', "
				+ "quantityOrderedPurchaseOrder: '$quantityOrderedPurchaseOrder', "
				+ "netPricePOPrice: '$netPricePOPrice', "
				+ "priceUnitPo: '$priceUnitPo', } }";
		Bson q1QryConv = BasicDBObject.parse(qConv);
		
		String qCalc = "{ $project: { purchaseOrderNumber:'$purchaseOrderNumber', "
				+ "value:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']} } }";
		Bson q1QryCalc = BasicDBObject.parse(qCalc);
		
		String qSum = "{$group:{_id:null, total:{$sum:'$value'}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData").aggregate(Arrays.
				asList(
						q1QryConv
						, q1QryCalc
						, q1QrySum
				)
			)
		;
		
		
		double totalOV = ret.first().getDouble("total");
		
//		logger.debug("{}", totalOV);
		return totalOV;
	}
	
	
	public Map<String, Double> getAllOrderValueIssuedByDimension(String dimName) {

		Map<String, Double> retMap = new HashMap<>();
		
		
//		String qConv = "{$project:{dimName:'$"+dimName+"', quantityOrderedPurchaseOrder: "
//				+ "{$toDouble:'$quantityOrderedPurchaseOrder'}, "
//				+ "netPricePOPrice: {$toDouble:'$netPricePOPrice'}, "
//				+ "priceUnitPo: {$toDouble:'$priceUnitPo'}, } }";

		String qConv = "{$project:{dimName:'$"+dimName+"', quantityOrderedPurchaseOrder: "
				+ "'$quantityOrderedPurchaseOrder', "
				+ "netPricePOPrice: '$netPricePOPrice', "
				+ "priceUnitPo: '$priceUnitPo', } }";
		Bson q1QryConv = BasicDBObject.parse(qConv);
		
		String qCalc = "{ $project: { dimName:'$dimName', "
				+ "value:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']} } }";
		Bson q1QryCalc = BasicDBObject.parse(qCalc);
		
		String qSum = "{$group:{_id:'$dimName', total:{$sum:'$value'}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData").aggregate(Arrays.
				asList(
						q1QryConv
						, q1QryCalc
						, q1QrySum
				)
			)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
			retMap.put(doc.getString("_id"), doc.getDouble("total"));
		});
		
		Double totalValue = getAllOrderValueIssued();
		
		retMap.put("_TOTAL_", totalValue);
		
		return retMap;
	}


	public Map<String, Double> getAllOrderValueIssuedByDateRange(String startDate, String enddate) {

		Map<String, Double> retMap = new TreeMap<>();
		
		String matchQry = "{$match:{$and:[{purchaseOrderCreationDate:{$gte:ISODate('"+startDate+"')}},{purchaseOrderCreationDate:{$lte:ISODate('"+enddate+"')}}]}}";
		Bson q1MatchQry = BasicDBObject.parse(matchQry);

		String qConv = "{$project:{"
				+ "purchaseOrderCreationDate:{$dateToString: { format: '%Y-%m-%d', date: '$purchaseOrderCreationDate' }}, "
				+ "quantityOrderedPurchaseOrder: '$quantityOrderedPurchaseOrder', "
				+ "netPricePOPrice: '$netPricePOPrice', "
				+ "priceUnitPo: '$priceUnitPo' } }";
		Bson q1QryConv = BasicDBObject.parse(qConv);
		
		String qCalc = "{ $project: { purchaseOrderCreationDate:'$purchaseOrderCreationDate', "
				+ "value:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']} } }";
		Bson q1QryCalc = BasicDBObject.parse(qCalc);
		
		String qSum = "{$group:{_id:'$purchaseOrderCreationDate', total:{$sum:'$value'}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		
//		String qSort = "{$sort:{$_id:1}}";
//		Bson q1Sort = BasicDBObject.parse(qSort);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData").aggregate(Arrays.
				asList(
						q1MatchQry
						, q1QryConv
						, q1QryCalc
						, q1QrySum
						//, q1Sort
				)
			)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
			retMap.put(doc.getString("_id"), doc.getDouble("total"));
		});
		return retMap;
	}
	
	
	
	public double getAllInvoiceValueIssued() {

//		String qConv = "{$project:{invoiceNumber:'$invoiceNumber', invoiceQuantity: {$toDouble:'$invoiceQuantity'}, "
//				+ "invoiceUnitPriceAsPerTc: {$toDouble:'$invoiceUnitPriceAsPerTc'}, priceUnitPo: {$toDouble:'$priceUnitPo'}, } }";
		String qConv = "{$project:{invoiceNumber:'$invoiceNumber', invoiceQuantity: '$invoiceQuantity', "
				+ "invoiceUnitPriceAsPerTc: '$invoiceUnitPriceAsPerTc', "
				+ "priceUnitPo: '$priceUnitPo', } }";
		Bson q1QryConv = BasicDBObject.parse(qConv);
		
		String qCalc = "{ $project: { invoiceNumber:'$invoiceNumber', "
				+ "value:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']} } }";
		Bson q1QryCalc = BasicDBObject.parse(qCalc);
		
		String qSum = "{$group:{_id:null, total:{$sum:'$value'}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData").aggregate(Arrays.
				asList(
						q1QryConv
						, q1QryCalc
						, q1QrySum
				)
			)
		;
		
		
		double totalOV = ret.first().getDouble("total");
		
		logger.debug("{}", totalOV);
		return totalOV;
	}

	
	public double getAllInvoiceValueIssuedByFilter(Map<String, String> filterMap, String date) {
		
		
		String qMatch = common.formMatchClauseForFilter(filterMap, "invoiceDate", date);		
		
		Bson q1QryMatch = BasicDBObject.parse(qMatch);
		
		String qConv = "{$project:{invoiceNumber:'$invoiceNumber', invoiceQuantity: '$invoiceQuantity', "
				+ "invoiceUnitPriceAsPerTc: '$invoiceUnitPriceAsPerTc', "
				+ "priceUnitPo: '$priceUnitPo', } }";
		Bson q1QryConv = BasicDBObject.parse(qConv);
		
		String qCalc = "{ $project: { invoiceNumber:'$invoiceNumber', "
				+ "value:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']} } }";
		Bson q1QryCalc = BasicDBObject.parse(qCalc);
		
		String qSum = "{$group:{_id:null, total:{$sum:'$value'}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData").aggregate(Arrays.
				asList(
						q1QryMatch
						, q1QryConv
						, q1QryCalc
						, q1QrySum
				)
			)
		;
		
		
		
		double totalOV = 0.0;
		if( ret.first()!=null && ret.first().getDouble("total")!=null)
		totalOV = ret.first().getDouble("total");
		
		logger.debug("{}", totalOV);
		return totalOV;
	}

	public double getAllOrderValueIssuedByFilter(Map<String, String> filterMap, String date) {

		String qMatch = common.formMatchClauseForFilter(filterMap, "purchaseOrderCreationDate", date);
		Bson q1QryMatch = BasicDBObject.parse(qMatch);

		String qConv = "{$project:{purchaseOrderNumber:'$purchaseOrderNumberOne', "
				+ "quantityOrderedPurchaseOrder: '$quantityOrderedPurchaseOrder', "
				+ "netPricePOPrice: '$netPricePOPrice', "
				+ "priceUnitPo: '$priceUnitPo', } }";
		Bson q1QryConv = BasicDBObject.parse(qConv);
		
		String qCalc = "{ $project: { purchaseOrderNumber:'$purchaseOrderNumber', "
				+ "value:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']} } }";
		Bson q1QryCalc = BasicDBObject.parse(qCalc);
		
		String qSum = "{$group:{_id:null, total:{$sum:'$value'}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData").aggregate(Arrays.
				asList(
						q1QryMatch
						, q1QryConv
						, q1QryCalc
						, q1QrySum
				)
			)
		;
		
		
		double totalOV = 0.0;
		try {
			totalOV = ret.first().getDouble("total");
		}catch(NullPointerException nx) {
			
		}
		
		return totalOV;
	}

	public Map<String, Double> getAllInvoiceValueIssuedByDimension(String dimName) {

		Map<String, Double> retMap = new HashMap<>();
		
		
		String qConv = "{$project:{dimName:'$"+dimName+"', "
				+ "invoiceQuantity: '$invoiceQuantity', "
				+ "invoiceUnitPriceAsPerTc: '$invoiceUnitPriceAsPerTc', "
				+ "priceUnitPo: '$priceUnitPo', } }";
		Bson q1QryConv = BasicDBObject.parse(qConv);

		String qCalc = "{ $project: { dimName:'$dimName', "
				+ "value:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']} } }";
		Bson q1QryCalc = BasicDBObject.parse(qCalc);
		
		String qSum = "{$group:{_id:'$dimName', total:{$sum:'$value'}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData").aggregate(Arrays.
				asList(
						q1QryConv
						, q1QryCalc
						, q1QrySum
				)
			)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
			retMap.put(doc.getString("_id"), doc.getDouble("total"));
		});

		Double totalValue = getAllInvoiceValueIssued();
		
		retMap.put("_TOTAL_", totalValue);
		
		return retMap;
	}


//	public Map<String, Double> getAllInvoiceValueIssuedByDateRange(String startDate, String enddate) {
//
//		Map<String, Double> retMap = new TreeMap<>();
//		
//		String matchQry = "{$match:{$and:[{invoiceDate:{$gte:ISODate('"+startDate+"')}},{invoiceDate:{$lte:ISODate('"+enddate+"')}}]}}";
//		Bson q1MatchQry = BasicDBObject.parse(matchQry);
//
//		String qConv = "{$project:{"
//				+ "invoiceDate:{$dateToString: { format: '%Y-%m-%d', date: '$invoiceDate' }}, "
//				+ "invoiceQuantity: '$invoiceQuantity', "
//				+ "invoiceUnitPriceAsPerTc: '$invoiceUnitPriceAsPerTc', "
//				+ "priceUnitPo: '$priceUnitPo', } }";
//		Bson q1QryConv = BasicDBObject.parse(qConv);
//		
//		String qCalc = "{ $project: { invoiceDate:'$invoiceDate', value:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, "
//				+ "'$invoiceQuantity']} } }";
//		Bson q1QryCalc = BasicDBObject.parse(qCalc);
//		
//		String qSum = "{$group:{_id:'$invoiceDate', total:{$sum:'$value'}}}";
//		Bson q1QrySum = BasicDBObject.parse(qSum);
//		
//		String qSort = "{$sort:{$_id:1}}";
//		Bson q1Sort = BasicDBObject.parse(qSort);
//		
//		MongoDatabase mongo = mongoTemplate.getDb();
//		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData").aggregate(Arrays.
//				asList(
//						q1MatchQry
//						, q1QryConv
//						, q1QryCalc
//						, q1QrySum
////						, q1Sort
//				)
//			)
//		;
//		
//		
//		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
//			retMap.put(doc.getString("_id"), doc.getDouble("total"));
//		});
//		return retMap;
//	}
	
	public Map<String, Double> getAllInvoiceValueIssuedByDateRange(String startDate, String enddate) {

		Map<String, Double> retMap = new TreeMap<>();
		
		String matchQry = "{$match:{$and:[{invoiceDate:{$gte:ISODate('"+startDate+"')}},{invoiceDate:{$lte:ISODate('"+enddate+"')}}]}}";
		Bson q1MatchQry = BasicDBObject.parse(matchQry);

		String qConv = "{$project:{"
				+ "gendate:{$dateToString: { format: '%Y-%m-%d', date: '$invoiceDate' }}, "
				+ "qty: '$invoiceQuantity', "
				+ "unitpr: '$invoiceUnitPriceAsPerTc', "
				+ "priceUnit: '$priceUnitPo', } }";
		Bson q1QryConv = BasicDBObject.parse(qConv);
		
		String qCalc = "{ $project: { gendate:'$gendate', value:{$multiply:[{$divide:['$unitpr', '$priceUnit']}, "
				+ "'$qty']} } }";
		Bson q1QryCalc = BasicDBObject.parse(qCalc);
		
		String qSum = "{$group:{_id:'$gendate', total:{$sum:'$value'}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		
//		String qSort = "{$sort:{$_id:1}}";
//		Bson q1Sort = BasicDBObject.parse(qSort);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData").aggregate(Arrays.
				asList(
						q1MatchQry
						, q1QryConv
						, q1QryCalc
						, q1QrySum
//						, q1Sort
				)
			)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
			retMap.put(doc.getString("_id"), doc.getDouble("total"));
		});
		return retMap;
	}
	
	
	public int getTotalNumberOfOrders() {
		List<Bson> pipeLine = new ArrayList<Bson>(); 

		String qSum = "{$group:{_id:'$purchaseOrderNumberOne', total:{$sum:1}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		pipeLine.add(q1QrySum);

		String qCount = "{$group:{_id:null, total:{$sum:1}}}";
		Bson qQCount = BasicDBObject.parse(qCount);
		pipeLine.add(qQCount);
		
		
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData")
				.aggregate(pipeLine);
		
		int num = 0;
		try {
			num = ret.first().getInteger("total");
		} catch (NullPointerException e) {
			// TODO: handle exception
		}
		return num;
	}
	
	public Map<String, Integer> getTotalNumberOfOrdersByDateRange(String startDate, String endDate) {
		List<Bson> pipeLine = new ArrayList<Bson>(); 

		String matchQry = "{$match:{$and:[{purchaseOrderCreationDate:{$gte:ISODate('"+startDate+"')}},{purchaseOrderCreationDate:{$lte:ISODate('"+endDate+"')}}]}}";
		Bson q1MatchQry = BasicDBObject.parse(matchQry);
		pipeLine.add(q1MatchQry);

		String qConv = "{$project:{"
				+ "gendate:{$dateToString: { format: '%Y-%m-%d', date: '$purchaseOrderCreationDate' }}, "
				+ "} }";
		Bson q1QryConv = BasicDBObject.parse(qConv);
		pipeLine.add(q1QryConv);

		
		String qSum = "{$group:{_id:'$gendate', count:{$sum:1}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		pipeLine.add(q1QrySum);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData")
				.aggregate(pipeLine);
		

		Map<String, Integer> retMap = new TreeMap<>();
		ret.cursor().forEachRemaining(doc->{
			retMap.put(doc.getString("_id"), doc.getInteger("count"));
		});
		
		return retMap;
	}
	
	public int getTotalNumberOfOrdersByFilters(Map<String, String> filterMap, String date) {
		List<Bson> pipeLine = new ArrayList<Bson>(); 
		

		String qMatch = common.formMatchClauseForFilter(filterMap, "purchaseOrderCreationDate", date);
		Bson q1QryMatch = BasicDBObject.parse(qMatch);
		pipeLine.add(q1QryMatch);
		
		String qSum = "{$group:{_id:'$purchaseOrderNumberOne', total:{$sum:1}}}";
		Bson q1QrySum = BasicDBObject.parse(qSum);
		pipeLine.add(q1QrySum);

		String qCount = "{$group:{_id:null, total:{$sum:1}}}";
		Bson qQCount = BasicDBObject.parse(qCount);
		pipeLine.add(qQCount);
		
		
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection("purchaseOrderInvoiceData")
				.aggregate(pipeLine);
		
		int num = 0;
		try {
			num = ret.first().getInteger("total");
		} catch (NullPointerException e) {
			// TODO: handle exception
		}
		return num;
	}
	
}
