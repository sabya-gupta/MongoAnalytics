package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.regex;

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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;

@Repository
public class KPIFilterAndGroupByHandlerForVoucher {

	String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;
	
	@Autowired
	Common common;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	MongoTemplate mongoTemplate;
	
	@Autowired
	KPIFilterAndGroupByHandler kPIFilterAndGroupByHandler;
	
	public Map<String, Map<String, Double>> getTotalVoucherValue(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second, String searchStr, boolean isConsumed) {

		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			filter.putAll(argfilter);
		}

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		Bson firstProjectVLBson = BasicDBObject
				.parse("{$project:{"
						+ "_id: 0, "
						+ "ivu:'$invoiceUnitPriceAsPerTc', "
						+ "poN:'$purchaseOrderNumberOne', "
						+ "poQ: '$quantityOrderedPurchaseOrder', "
						+ "net: '$netPricePOPrice', "
						+ "priceUnitPo: '$priceUnitPo', "
						+ "idyy : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
						+ "podtyy : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }},"
						+ "iddd : {$dateToString: { format: '%Y-%m-%d', date: '$invoiceDate' }}, "
						+ "podtdd : {$dateToString: { format: '%Y-%m-%d', date: '$purchaseOrderCreationDate' }},"
						+ "vouch : '$voucherConsumed'"
						+ "supplierPartNumber: '$supplierPartNumber', "
						+ "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " 
						+ "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " 
						+ "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " 
						+ "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," 
				+ "} }");
		pipeline.add(firstProjectVLBson);

//		printDocs(collectionName, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("podtyy", yyyymm));
			pipeline.add(dateFilter);
		}

//		//now unwind
		pipeline.add(Aggregates.unwind("$vouch"));


//		printDocs(collectionName, pipeline);


		
		Bson secondProjectVLBson = BasicDBObject.parse("{$group:{_id:'$"+groupByPropName+"', "+
		Constants.VOUCHER_CONSUMED+":{'$sum':'$vouch.consumed'}, "+Constants.VOUCHER_REMAINING+":{'$sum':'$vouch.remaining'}}}");
		pipeline.add(secondProjectVLBson);

		printDocs(collectionName, pipeline);


		List<String> kpisV = new ArrayList<String>();
		kpisV.add(Constants.VOUCHER_CONSUMED);
		kpisV.add(Constants.VOUCHER_REMAINING);
		String sortByField = isConsumed ? Constants.VOUCHER_CONSUMED : Constants.VOUCHER_REMAINING;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, collectionName, sortByField, mongoTemplate);

		if (!second) {
			kPIFilterAndGroupByHandler.getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			kPIFilterAndGroupByHandler.getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			kPIFilterAndGroupByHandler.getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			kPIFilterAndGroupByHandler.getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			kPIFilterAndGroupByHandler.getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			kPIFilterAndGroupByHandler.getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

		}

		logger.debug("FINAL    FINAL   LEAKAGE 3 {}", retMap);

		return retMap;
	}

	public int getTotalVoucherItemsCOUNT(String groupByPropName, Map<String, List<String>> argfilter, 
			List<String> yyyymm, String searchStr) {

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			filter.putAll(argfilter);
		}

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		Bson firstProjectVLBson = BasicDBObject
				.parse("{$project:{"
						+ "_id: 0, "
						+ "ivu:'$invoiceUnitPriceAsPerTc', "
						+ "poN:'$purchaseOrderNumberOne', "
						+ "poQ: '$quantityOrderedPurchaseOrder', "
						+ "net: '$netPricePOPrice', "
						+ "priceUnitPo: '$priceUnitPo', "
						+ "idyy : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
						+ "podtyy : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }},"
						+ "iddd : {$dateToString: { format: '%Y-%m-%d', date: '$invoiceDate' }}, "
						+ "podtdd : {$dateToString: { format: '%Y-%m-%d', date: '$purchaseOrderCreationDate' }},"
						+ "vouch : '$voucherConsumed'"
						+ "supplierPartNumber: '$supplierPartNumber', "
						+ "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " 
						+ "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " 
						+ "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " 
						+ "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," 
				+ "} }");
		pipeline.add(firstProjectVLBson);

//		printDocs(collectionName, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("podtyy", yyyymm));
			pipeline.add(dateFilter);
		}

//		//now unwind
		pipeline.add(Aggregates.unwind("$vouch"));


//		printDocs(collectionName, pipeline);


		
		Bson secondProjectVLBson = BasicDBObject.parse("{$group:{_id:'$"+groupByPropName+"', "+
		Constants.VOUCHER_CONSUMED+":{'$sum':'$vouch.consumed'}, "+Constants.VOUCHER_REMAINING+":{'$sum':'$vouch.remaining'}}}");
		pipeline.add(secondProjectVLBson);

		printDocs(collectionName, pipeline);


		int count = 0;
		count = getCount(collectionName, pipeline);
		
		logger.debug("The count is = {}", count);
		return count;

	}
	
	private int getCount(String collection, List<Bson> pipeline) {

		List<Bson> newpipeline = new ArrayList<Bson>();
		newpipeline.addAll(pipeline);

		String cntStr = "{$group:{_id:null, val:{'$sum':1}}}";
		newpipeline.add(BasicDBObject.parse(cntStr));

		MongoDatabase mongo = mongoTemplate.getDb();

		int count = 0;

		try {
//			printDocs(collection, pipeline);
//			printDocs(collection, newpipeline);
			count = mongo.getCollection(collection).aggregate(newpipeline).first().getInteger("val");
		} catch (NullPointerException e) {

		} catch (Exception e) {
			e.printStackTrace();
		}

		return count;
	}

	private void printDocs(String collection, List<Bson> pipeline) {
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug(">>>>>>>>>>{}", doc);
		});

	}

}
