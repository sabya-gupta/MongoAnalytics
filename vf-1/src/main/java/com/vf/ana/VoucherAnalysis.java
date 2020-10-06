package com.vf.ana;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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

@Service
public class VoucherAnalysis {

	@Autowired
	Common common;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	MongoTemplate mongoTemplate;

	public List<Map<String, String>> renderVoucherGrid(String voucherId) {

		String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
		List<Bson> pipeline = new ArrayList<>();

		Bson firstProjectVLBson = BasicDBObject.parse("" + "{$project: {" 
				+ "_id: 0," 
				+ "supplierPartNumber:1,"
				+ "materialGroupL4:1," 
				+ "purchaseOrderNumberOne:1,"
				+ "quantityOrderedPurchaseOrder: 1, "
				+ "invoiceQuantity: 1,"
				+ "purchaseOrderCreationDate:{$dateToString: { format: '%d/%m/%Y', date: '$purchaseOrderCreationDate' }},"
				+ "poValue:{'$multiply':[{'$divide':['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']},"
				+ "invoiceNumber:1," 
				+ "invoiceDate:{$dateToString: { format: '%d/%m/%Y', date: '$invoiceDate' }},"
				+ "invValue:{'$multiply':[{'$divide':['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']},"
				+ "voucherConsumed:1" 
				+ "}}");
		pipeline.add(firstProjectVLBson);


		pipeline.add(Aggregates.unwind("$voucherConsumed"));

		
		
		Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" 
				+ "supplierPartNumber:1," 
				+ "materialGroupL4:1,"
				+ "purchaseOrderNumberOne:1," 
				+ "purchaseOrderCreationDate:1," 
				+ "poValue:1," 
				+ "invoiceNumber:1,"
				+ "invoiceDate:1," 
				+ "invValue: 1, " 
				+ "redeemedVal: '$voucherConsumed.consumed', "
				+ "voucherid: '$voucherConsumed.voucherId', "
				+ "voucherLimitation: '$voucherConsumed.voucherLimitation', " 
				+ "quantityOrderedPurchaseOrder: 1, "
				+ "invoiceQuantity: 1"
				+ "} }");
		pipeline.add(secondProjectVLBson);
		
		// filter by voucher id
		pipeline.add(Aggregates.match(Filters.eq("voucherid", voucherId)));		
//		common.printDocs(collectionName, pipeline, mongoTemplate);
		
		String grpByLV = "{$group:{_id: {" 
				+ "supplierPartNumber:'$supplierPartNumber',"
				+ "materialGroupL4:'$materialGroupL4'," 
				+ "purchaseOrderNumberOne:'$purchaseOrderNumberOne',"
				+ "purchaseOrderCreationDate:'$purchaseOrderCreationDate'," 
				+ "poValue:'$poValue',"
				+ "invoiceNumber:'$invoiceNumber'," 
				+ "invoiceDate:'$invoiceDate'," 
				+ "invValue:'$invValue', "
				+ "voucherid : '$voucherid'," 
				+ "voucherLimitation: '$voucherLimitation'" 
				+ "quantityOrderedPurchaseOrder: '$quantityOrderedPurchaseOrder', "
				+ "invoiceQuantity: '$invoiceQuantity'"
				+ "} , "
				+ "val:{'$sum':'$redeemedVal'}" 
				+ "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		String projectOut = "{$project:{" 
				+ "supplierPartNumber:'$_id.supplierPartNumber',"
				+ "materialGroupL4:'$_id.materialGroupL4'," 
				+ "purchaseOrderNumberOne:'$_id.purchaseOrderNumberOne',"
				+ "purchaseOrderCreationDate:'$_id.purchaseOrderCreationDate'," 
				+ "poValue:'$_id.poValue',"
				+ "invoiceNumber:'$_id.invoiceNumber'," 
				+ "invoiceDate:'$_id.invoiceDate'," 
				+ "invValue:'$_id.invValue', "
				+ "voucherid : '$_id.voucherid'," 
				+ "voucherLimitation: '$_id.voucherLimitation'" 
				+ "quantityOrderedPurchaseOrder: '$_id.quantityOrderedPurchaseOrder', "
				+ "invoiceQuantity: '$_id.invoiceQuantity'"
				+ "val:'$val'" 
				+ "}}";

		pipeline.add(BasicDBObject.parse(projectOut));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		List<Map<String, String>> retList = new ArrayList<>();
		ret.cursor().forEachRemaining(doc -> {
//			logger.debug("<<<{}", doc);
//			logger.debug(">>>{}", ((Document)doc.get("_id")).getString("supplierPartNumber"));
			Map<String, String> tmpMap = new HashMap<>();
			tmpMap.put("supplierPartNumber", doc.getString("supplierPartNumber"));
			tmpMap.put("materialGroupL4", doc.getString("materialGroupL4"));
			tmpMap.put("purchaseOrderNumberOne", doc.getString("purchaseOrderNumberOne"));
			tmpMap.put("purchaseOrderCreationDate", doc.getString("purchaseOrderCreationDate"));

			Double val = doc.getDouble("poValue");
			tmpMap.put("poValue", val.toString());

			tmpMap.put("invoiceNumber", doc.getString("invoiceNumber"));
			tmpMap.put("invoiceDate", doc.getString("invoiceDate"));			

			val = doc.getDouble("invValue");
			tmpMap.put("invValue", val.toString());
			
			tmpMap.put("voucherid", doc.getString("voucherid"));
			
			val = doc.getDouble("voucherLimitation");
			tmpMap.put("voucherLimitation", val.toString());

			val = doc.getDouble("val");
			tmpMap.put("val", val.toString());
			retList.add(tmpMap);
		});

		logger.debug("{}", retList);
		return retList;
	}

	
	public Map<String, String> getVoucherDetails(String voucherId) {
		String collectionName=Constants.VALUE_LEAKAGE_MASTER_COLLECTION_NAME;
		Document doc = mongoTemplate.getCollection(collectionName).find(Filters.eq("voucherId", voucherId)).first();
		Map<String, String> retMap = new HashMap<>();
		retMap.put("totalVoucherValue", doc.getDouble("totalValue").toString());
		Double bal = doc.getDouble("totalValue") - doc.getDouble("consumedValue");
		retMap.put("availableBalance", bal.toString());
		
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		Date dt = doc.getDate("startDate");
		retMap.put("validFrom", sdf.format(dt));

		dt = doc.getDate("endDate");
		retMap.put("validTo", sdf.format(dt));

		retMap.put("poref", doc.getString("voucherApplicationReference"));
		
		logger.debug("{}", retMap);
		return retMap;
	}

}
