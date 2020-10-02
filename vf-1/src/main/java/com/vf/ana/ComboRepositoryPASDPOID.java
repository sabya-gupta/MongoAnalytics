package com.vf.ana;

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
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;


@Repository
public class ComboRepositoryPASDPOID {

	@Autowired
	MongoTemplate mongoTemplate;
	
	@Autowired
	Common common;
	
	Logger logger = LoggerFactory.getLogger(getClass());
	public Map<String, Map<String, Double>> getAllMeasuresFromPOIDForAPropertyForAllPropValues() {
		
		Map<String, Map<String, Double>> retMap = new HashMap<>();
		
		List<Bson> pipeLine = new ArrayList<Bson>(); 
		
		//OV and IV
		String projectQry = "{ $project: {"+
			"parn: '$priceAgreementReferenceName', spn:'$supplierPartNumber', tm:'$tradingModel', psn:'$parentSupplierName', sid:'$supplierId', "
			+ "opco: '$opcoCode', catType: '$catalogueType', l4: '$materialGroupL4', poCurr: '$purchaseOrderCurrency', "
			+ "poCurr2:'$purchaseOrderTwoCurrency', poDt: '$purchaseOrderCreationDate', "
			+ "invDt: '$invoiceDate'"+
			"valueOV:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']},"+
			"valueIV:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']},"+
			"} }";	
		Bson bsonProjectQry = BasicDBObject.parse(projectQry);
		pipeLine.add(bsonProjectQry);
		
		String groupQry = "{$group:{_id:{parn:'$parn', spn:'$spn', tm:'$tm', psn:'$psn', sid:'$sid', opco:'$opco', catType:'$catType', " + 
				"l4:'$l4', poCurr:'$poCurr', poCurr2:'$poCurr2', poDt:'$poDt', invDt:'$invDt'}, totalOV:{$sum:'$valueOV'},  totalIV:{$sum:'$valueIV'} }}";
		Bson bsonGroupQry = BasicDBObject.parse(groupQry);

		pipeLine.add(bsonGroupQry);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		ret.cursor().forEachRemaining(doc->{
			logger.debug("{}", doc);
//			Map<String, Double> tmpMap = new HashMap<>();
//			tmpMap.put(Constants.ORDER_VALUE, doc.getDouble("totalOV"));
//			tmpMap.put(Constants.INVOICE_VALUE, doc.getDouble("totalIV"));
//			retMap.put(doc.getString("_id"), tmpMap);
		});

		
		pipeLine.clear();
//		Num PO

		projectQry = "{ $project: {"+
				"parn: '$priceAgreementReferenceName', spn:'$supplierPartNumber', tm:'$tradingModel', psn:'$parentSupplierName', sid:'$supplierId', "
				+ "opco: '$opcoCode', catType: '$catalogueType', l4: '$materialGroupL4', poCurr: '$purchaseOrderCurrency', "
				+ "poCurr2:'$purchaseOrderTwoCurrency', poDt: '$purchaseOrderCreationDate', "
				+ "invDt: '$invoiceDate'"+
				"po:'$purchaseOrderNumberOne'"+
				"} }";	
		bsonProjectQry = BasicDBObject.parse(projectQry);
		pipeLine.add(bsonProjectQry);
		
		
		
		groupQry = "{$group:{_id:{parn:'$parn', spn:'$spn', tm:'$tm', psn:'$psn', sid:'$sid', opco:'$opco', catType:'$catType', " + 
				"l4:'$l4', poCurr:'$poCurr', poCurr2:'$poCurr2', poDt:'$poDt', invDt:'$invDt'}, totalPO:{$sum:1} }}";
		bsonGroupQry = BasicDBObject.parse(groupQry);

		pipeLine.add(bsonGroupQry);
//		
//		String groupQryDistPO = "{$group: {_id: {dim:'$_id.dim'}, totalPO:{$sum:1} }}";
//		Bson bsonGroupDistQryPO = BasicDBObject.parse(groupQryDistPO);
//
//		pipeLine.add(bsonGroupDistQryPO);
//		
//		
		mongo = mongoTemplate.getDb();
		AggregateIterable<Document> retPO = mongo.getCollection(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		retPO.cursor().forEachRemaining(doc->{
			logger.debug("{}", doc);
//			String key = ((Document)doc.get("_id")).getString("dim");
//			Map<String, Double> tmpMap = retMap.get(key);
//			if(tmpMap==null) {
//				tmpMap = new HashMap<>();
//			}
//			tmpMap.put(Constants.NUMBER_OF_ORDERS, doc.getInteger("totalPO").doubleValue());
//			retMap.put(key, tmpMap);
		});
		
		
		pipeLine.clear();
//		Num PO

		projectQry = "{ $project: {"+
				"parn: '$priceAgreementReferenceName', spn:'$supplierPartNumber', tm:'$tradingModel', psn:'$parentSupplierName', sid:'$supplierId', "
				+ "opco: '$opcoCode', catType: '$catalogueType', l4: '$materialGroupL4', poCurr: '$purchaseOrderCurrency', "
				+ "poCurr2:'$purchaseOrderTwoCurrency', poDt: '$purchaseOrderCreationDate', "
				+ "invDt: '$invoiceDate', vf: '$validFromDate', vt:'$validToDate'"+
				"po:'$purchaseOrderNumberOne'"+
				"} }";	
		bsonProjectQry = BasicDBObject.parse(projectQry);
		pipeLine.add(bsonProjectQry);
		
		
		
		groupQry = "{$group:{_id:{spn:'$spn', tm:'$tm', psn:'$psn', sid:'$sid', opco:'$opco', catType:'$catType', " + 
				"l4:'$l4', poCurr:'$poCurr', poCurr2:'$poCurr2', poDt:'$poDt', invDt:'$invDt'}, totalPO:{$sum:1} }}";
		bsonGroupQry = BasicDBObject.parse(groupQry);

		pipeLine.add(bsonGroupQry);
//		
//		String groupQryDistPO = "{$group: {_id: {dim:'$_id.dim'}, totalPO:{$sum:1} }}";
//		Bson bsonGroupDistQryPO = BasicDBObject.parse(groupQryDistPO);
//
//		pipeLine.add(bsonGroupDistQryPO);
//		
//		
		mongo = mongoTemplate.getDb();
		AggregateIterable<Document> retPA = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME).
				aggregate(pipeLine)
		;
		
		
		retPA.cursor().forEachRemaining(doc->{
			logger.debug(">>>>>>>>>{}", doc);
//			String key = ((Document)doc.get("_id")).getString("dim");
//			Map<String, Double> tmpMap = retMap.get(key);
//			if(tmpMap==null) {
//				tmpMap = new HashMap<>();
//			}
//			tmpMap.put(Constants.NUMBER_OF_ORDERS, doc.getInteger("totalPO").doubleValue());
//			retMap.put(key, tmpMap);
		});

		return retMap;
	}
	
	
}
