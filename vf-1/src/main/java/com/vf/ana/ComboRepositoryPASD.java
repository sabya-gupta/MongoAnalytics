package com.vf.ana;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.vf.ana.ent.AggregatedDataFirstLevel;

@Repository
public class ComboRepositoryPASD {

	@Autowired
	MongoTemplate mongoTemplate;
	
	@Autowired
	Common common;
	
	Logger logger = LoggerFactory.getLogger(getClass());

	
	

	/**
	 * Given a property this function returns a hashmap containing 
	 * 1.Total Order Value, 2.Total Invoice Value and 3.Number of purchase order 
	 * for each values for the property from the purchaseOrderInvoiceData collection
	 * 
	 */
	
	public Map<String, Map<String, Double>> getAllMeasuresFromPOIDForAPropertyForAllPropValues(String dimName) {
		
		Map<String, Map<String, Double>> retMap = new HashMap<>();
		
		List<Bson> pipeLine = new ArrayList<Bson>(); 
		
		//OV and IV
		String projectQry = "{ $project: {"+
			"dim: '$"+dimName+"', "+
			"valueOV:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']},"+
			"valueIV:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']},"+
			"} }";	
		Bson bsonProjectQry = BasicDBObject.parse(projectQry);
		pipeLine.add(bsonProjectQry);
		
		String groupQry = "{$group:{_id:'$dim', totalOV:{$sum:'$valueOV'},  totalIV:{$sum:'$valueIV'}, totalPO:{$sum:1}  }}";
		Bson bsonGroupQry = BasicDBObject.parse(groupQry);

		pipeLine.add(bsonGroupQry);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
			Map<String, Double> tmpMap = new HashMap<>();
			tmpMap.put(Constants.ORDER_VALUE, doc.getDouble("totalOV"));
			tmpMap.put(Constants.INVOICE_VALUE, doc.getDouble("totalIV"));
//			tmpMap.put(Constants.NUMBER_OF_ORDERS, doc.getInteger("totalPO").doubleValue());
			retMap.put(doc.getString("_id"), tmpMap);
		});

		
		pipeLine.clear();
		//Num PO
		
		String groupQryPO = "{$group: {_id: {dim:'$"+dimName+"', po:'$purchaseOrderNumberOne'}, count:{$sum:1} }}";
		Bson bsonGroupQryPO = BasicDBObject.parse(groupQryPO);

		pipeLine.add(bsonGroupQryPO);
		
		String groupQryDistPO = "{$group: {_id: {dim:'$_id.dim'}, totalPO:{$sum:1} }}";
		Bson bsonGroupDistQryPO = BasicDBObject.parse(groupQryDistPO);

		pipeLine.add(bsonGroupDistQryPO);
		
//		logger.debug("{}", pipeLine);
		
		mongo = mongoTemplate.getDb();
		AggregateIterable<Document> retPO = mongo.getCollection(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		retPO.cursor().forEachRemaining(doc->{
//			logger.debug(">>{}", doc);
			String key = ((Document)doc.get("_id")).getString("dim");
			Map<String, Double> tmpMap = retMap.get(key);
			if(tmpMap==null) {
				tmpMap = new HashMap<>();
			}
			tmpMap.put(Constants.NUMBER_OF_ORDERS, doc.getInteger("totalPO").doubleValue());
			retMap.put(key, tmpMap);
		});
		
		
		return retMap;
	}
	
	
	
	
	
	
	/**
	 * This function returns a hashmap of total values for 
	 * 1.Total Order Value, 2.Total Invoice Value and 3.Number of purchase order 
	 * from the purchaseOrderInvoiceData collection
	 * 
	 */
	public Map<String, Map<String, Double>> getTotalOfAllMeasuresFromPOID() {
		
		Map<String, Map<String, Double>> retMap = new HashMap<>();
		
		List<Bson> pipeLine = new ArrayList<Bson>(); 
		
		//OV and IV
		String projectQry = "{ $project: {"+
			"valueOV:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']},"+
			"valueIV:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']},"+
			"po:'$purchaseOrderNumberOne'"+
			"} }";	
		Bson bsonProjectQry = BasicDBObject.parse(projectQry);
		pipeLine.add(bsonProjectQry);
		
		String groupQry = "{$group:{_id:null, totalOV:{$sum:'$valueOV'},  totalIV:{$sum:'$valueIV'}  }}";
		Bson bsonGroupQry = BasicDBObject.parse(groupQry);

		pipeLine.add(bsonGroupQry);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
			Map<String, Double> tmpMap = new HashMap<>();
			tmpMap.put(Constants.ORDER_VALUE, doc.getDouble("totalOV"));
			tmpMap.put(Constants.INVOICE_VALUE, doc.getDouble("totalIV"));
//			tmpMap.put(Constants.NUMBER_OF_ORDERS, doc.getInteger("totalPO").doubleValue());
			retMap.put(Constants.TOTAL, tmpMap);
		});
		
		
		
		
		pipeLine.clear();
		//Num PO
		
		String groupQryPO = "{$group: {_id: '$purchaseOrderNumberOne', count:{$sum:1} }}";
		Bson bsonGroupQryPO = BasicDBObject.parse(groupQryPO);

		pipeLine.add(bsonGroupQryPO);
		
		String groupQryDistPO = "{$group: {_id: null, totalPO:{$sum:1} }}";
		Bson bsonGroupDistQryPO = BasicDBObject.parse(groupQryDistPO);

		pipeLine.add(bsonGroupDistQryPO);
		
//		logger.debug("{}", pipeLine);
		
		mongo = mongoTemplate.getDb();
		AggregateIterable<Document> retPO = mongo.getCollection(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		retPO.cursor().forEachRemaining(doc->{
//			logger.debug(">>>>>>{}", doc);
			Map<String, Double> tmpMap = retMap.get(Constants.TOTAL);
			if(tmpMap==null) tmpMap = new HashMap<>();
			tmpMap.put(Constants.NUMBER_OF_ORDERS, doc.getInteger("totalPO").doubleValue());
			retMap.put(Constants.TOTAL, tmpMap);
		});
	
		
		return retMap;
	}
	
	
	
	
	
	/**
	 * Given a property this function returns a hashmap containing 
	 * 1.Number of Active Price Agreements and 2.Number of active Items 
	 * for each values for the property from the priceAgreementSpnDetails collection
	 * 
	 */
	public Map<String, Map<String, Double>> getAllMeasuresFromPASDForAPropertyForAllPropValues(String dimName) {
		Map<String, Map<String, Double>> retMap = new HashMap<>();
		
		List<Bson> pipeLine = new ArrayList<Bson>(); 
		//FIRST GET ALL THE ACTIVE PRICE AGREEMENTS
		String activeMatch = "{$match:{priceAgreementStatus:'Active'}}";
		Bson activeMatchBson = BasicDBObject.parse(activeMatch);
		pipeLine.add(activeMatchBson);

		String groupQry = "{$group:{_id:{dim:'$"+dimName+"', spn:'$supplierPartNumber', opco:'$opcoCode'}, count:{$sum:1}}}";
		Bson groupQryBson = BasicDBObject.parse(groupQry);
		pipeLine.add(groupQryBson);
		
		String countQry = "{$group: {_id:'$_id.dim', count:{$sum:1}}}";
		Bson countQryBson = BasicDBObject.parse(countQry);
		pipeLine.add(countQryBson);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
			Map<String, Double> tmpMap = new HashMap<>();
			tmpMap.put(Constants.ACTIVE_PRICE_AGREEMENT, doc.getInteger("count").doubleValue());
			String key = doc.getString("_id");
			if(key==null) key = Constants.CATALAOG_TYPE_NOT_PRESENT;
			retMap.put(key, tmpMap);
		});

		
		//THEN GET ALL THE ACTIVE ITEMS
		pipeLine.clear();
		
		activeMatch = "{$match:{priceAgreementStatus:'Active'}}";
		activeMatchBson = BasicDBObject.parse(activeMatch);
		pipeLine.add(activeMatchBson);
		
		groupQry = "{$group:{_id:{dim:'$"+dimName+"', spn:'$supplierPartNumber'}, count:{$sum:1}}}";
		groupQryBson = BasicDBObject.parse(groupQry);
		pipeLine.add(groupQryBson);
		
		countQry = "{$group: {_id:'$_id.dim', count:{$sum:1}}}";
		countQryBson = BasicDBObject.parse(countQry);
		pipeLine.add(countQryBson);
		
		mongo = mongoTemplate.getDb();
		ret = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
//			logger.debug("{}", doc);
			String key = doc.getString("_id");
			if(key==null) key = Constants.CATALAOG_TYPE_NOT_PRESENT;
			Map<String, Double> tmpMap = retMap.get(key);
			if(tmpMap==null) tmpMap = new HashMap<>();
			tmpMap.put(Constants.ACTIVE_ITEMS, doc.getInteger("count").doubleValue());
			retMap.put(key, tmpMap);
		});
		
		return retMap;
	}

	
	
	
	
	/**
	 * This function returns a hashmap of total values for 
	 * 1.Number of Active Price Agreements and 2.Number of active Items 
	 * from the priceAgreementSpnDetails collection
	 * 
	 */
	public Map<String, Map<String, Double>> getTotalOfAllMeasuresFromPASD() {
		
		Map<String, Map<String, Double>> retMap = new HashMap<>();
		
		List<Bson> pipeLine = new ArrayList<Bson>(); 
		//FIRST GET ALL THE ACTIVE PRICE AGREEMENTS
		String activeMatch = "{$match:{priceAgreementStatus:'Active'}}";
		Bson activeMatchBson = BasicDBObject.parse(activeMatch);
		pipeLine.add(activeMatchBson);
		
		String groupQry = "{$group:{_id:{spn:'$supplierPartNumber', opco:'$opcoCode'}, count:{$sum:1}}}";
		Bson groupQryBson = BasicDBObject.parse(groupQry);
		pipeLine.add(groupQryBson);
		
		String groupQryCount = "{$group:{_id:null, count:{$sum:1}}}";
		Bson groupQryCountBson = BasicDBObject.parse(groupQryCount);
		pipeLine.add(groupQryCountBson);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
//			logger.debug("1111{}", doc);
			Map<String, Double> tmpMap = new HashMap<>();
			tmpMap.put(Constants.ACTIVE_PRICE_AGREEMENT, doc.getInteger("count").doubleValue());
			retMap.put(Constants.TOTAL, tmpMap);
		});
		
		
		
		
		pipeLine.clear();
		//THEN GET ALL ACTIVE ITEMS
		
		activeMatch = "{$match:{priceAgreementStatus:'Active'}}";
		activeMatchBson = BasicDBObject.parse(activeMatch);
		pipeLine.add(activeMatchBson);
		
		groupQry = "{$group:{_id:{spn:'$supplierPartNumber'}, count:{$sum:1}}}";
		groupQryBson = BasicDBObject.parse(groupQry);
		pipeLine.add(groupQryBson);
		
		groupQryCount = "{$group:{_id:null, count:{$sum:1}}}";
		groupQryCountBson = BasicDBObject.parse(groupQryCount);
		pipeLine.add(groupQryCountBson);
		
		mongo = mongoTemplate.getDb();
		ret = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
			String key = Constants.TOTAL;
			Map<String, Double> tmpMap = retMap.get(key);
			if(tmpMap==null) tmpMap = new HashMap<>();
			tmpMap.put(Constants.ACTIVE_ITEMS, doc.getInteger("count").doubleValue());
			retMap.put(key, tmpMap);
		});
		
		return retMap;
	}
	
	

	
	@Autowired
	AggDataRepositoryFirstLevel aggDataRepositoryFirstLevel;
	
	public Map<String, Map<String, Double>> getAllMeasuresForFirstLevelPropValues(String dimName, 
			int pageNum, String sortByField, int dir) {
		
		logger.debug("AFTER CHANGE>>>>>>>>");
		Map<String, Map<String, Double>> retMap = new LinkedHashMap <>();
		PageRequest pg =  PageRequest.of(pageNum, Constants.PAGE_SIZE);
		Sort sort = Sort.by(sortByField);
		if(dir==Constants.SORT_DIRECTION_ASCENDING) {
			sort = sort.ascending();
		}else if(dir==Constants.SORT_DIRECTION_DESCENDING) {
			sort = sort.descending();			
		}
		List<AggregatedDataFirstLevel> lst = aggDataRepositoryFirstLevel.findAllByPropName(dimName, pg, sort);
		for(AggregatedDataFirstLevel obj : lst) {
			logger.debug("{}", obj);
			Map<String, Double> tmpMap = new HashMap<>();
			tmpMap.put(Constants.ORDER_VALUE, obj.getOrderValue());
			tmpMap.put(Constants.INVOICE_VALUE, obj.getInvoiceValue());
			tmpMap.put(Constants.NUMBER_OF_ORDERS, obj.getOrdersIssued());
			tmpMap.put(Constants.ACTIVE_ITEMS, obj.getActiveItems());
			tmpMap.put(Constants.ACTIVE_PRICE_AGREEMENT, obj.getActivePriceAgreements());
			retMap.put(obj.getPropVal(), tmpMap);
		}
		logger.debug("size = {}", lst);
		return retMap;
	}

	public Map<String, Map<String, Double>> getAllMeasuresForFirstLevelPropValues(String dimName) {
		
		logger.debug("AFTER CHANGE>>>>>>>>");
		Map<String, Map<String, Double>> retMap = new LinkedHashMap <>();
		List<AggregatedDataFirstLevel> lst = aggDataRepositoryFirstLevel.findAllByPropName(dimName);
		for(AggregatedDataFirstLevel obj : lst) {
			logger.debug("{}", obj);
			Map<String, Double> tmpMap = new HashMap<>();
			tmpMap.put(Constants.ORDER_VALUE, obj.getOrderValue());
			tmpMap.put(Constants.INVOICE_VALUE, obj.getInvoiceValue());
			tmpMap.put(Constants.NUMBER_OF_ORDERS, obj.getOrdersIssued());
			tmpMap.put(Constants.ACTIVE_ITEMS, obj.getActiveItems());
			tmpMap.put(Constants.ACTIVE_PRICE_AGREEMENT, obj.getActivePriceAgreements());
			retMap.put(obj.getPropVal(), tmpMap);
		}
		logger.debug("size = {}", lst);
		return retMap;
	}
	
	
	public Map<String, Integer> getCountOfAllItemsByPropValues(String... dimNames) {
		
		logger.debug("AFTER CHANGE>>>>>>>>");
		Map<String, Integer> retMap = new HashMap <>();
		for(String dim : dimNames) {
			int count = aggDataRepositoryFirstLevel.countByPropName(dim);
			logger.debug("{} - {}",dim, count);
			retMap.put(dim, count);
		}
		return retMap;
	}
	
	
	
}



