package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lte;

import java.time.LocalDate;
import java.time.YearMonth;
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
import com.mongodb.client.model.Sorts;


@Service
public class VoucherAnalyticsAllLevels {

	@Autowired
	MongoTemplate mongoTemplate;
	
	@Autowired
	Common common;
	
	Logger logger = LoggerFactory.getLogger(getClass());
	
	
	//For top level
	/**
	 * Total - with filter and dates and without filter and dates
	 */
	public Map<String, Double> getTotalVoucherKPIs(Map<String, List<String>> filterMap, List<String> yyyymm) {
		Map<String, Double> ret = new HashMap<>();
		
		final String collectionName = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;
		List<Bson> pipeLine = new ArrayList<>();

		if(filterMap!=null) {
			Bson q1QryMatch = common.formMatchClauseForListFilterBson(filterMap);
			pipeLine.add(q1QryMatch);
		}

		String qConvDate = "{$project:{"
				+ "_id: 0, "
				+ "supplierPartNumber: '$supplierPartNumber', "
				+ "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " 
				+ "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " 
				+ "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " 
				+ "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," 
				+ "vouch : '$appliedVoucher'"
		+ "} }";
		
		pipeLine.add(BasicDBObject.parse(qConvDate));
		
		
		
//		pipeLine.add(BasicDBObject.parse("{'$limit':10}"));
//		common.printDocs(collectionName, pipeLine, mongoTemplate);

		//now unwind
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeLine.add(Aggregates.unwind("$vouch"));
//		common.printDocs(collectionName, pipeLine, mongoTemplate);

		qConvDate = "{$project:{"
				+ "_id: 0, "
				+ "supplierPartNumber: '$supplierPartNumber', "
				+ "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " 
				+ "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " 
				+ "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " 
				+ "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," 
				+ "vouchstartdt : '$vouch.startDate' , "
				+ "vouchenddt : '$vouch.endDate' , "
//				+ "vouchstartyymm : {$dateToString: { format: '%Y-%m', date: '$vouch.startDate' }, "
//				+ "vouchendyymm : {$dateToString: { format: '%Y-%m', date: '$vouch.endDate' }, "
				+ "vouchval : '$vouch.totalValue', "
		+ "} }";
		
		pipeLine.add(BasicDBObject.parse(qConvDate));			
//		common.printDocs(collectionName, pipeLine, mongoTemplate);
		
		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			List<Bson> orFilters = new ArrayList<>();
			for(String ym: yyyymm) {
				LocalDate startDate = LocalDate.parse(ym+"-01", DateTimeFormatter.ISO_DATE);
				YearMonth month = YearMonth.from(startDate);
				LocalDate endDate   = month.atEndOfMonth();
				
				Bson dateFilter = Filters.and(Filters.lte("vouchstartdt", endDate), Filters.gte("vouchenddt", startDate));
				orFilters.add(dateFilter);
			}
			Bson dateFilter = match(Filters.or(orFilters));
			pipeLine.add(dateFilter);
		}
		

		//now Sum them up
		pipeLine.add(BasicDBObject.parse("{$group:{_id:null, con:{'$sum':'$vouchval'}}}"));
		
		printDocs(collectionName, pipeLine);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> rec = mongo.getCollection(collectionName).aggregate(
				pipeLine
			)
		;
		
		
		try {
			double consumed = rec.first().getDouble("con");
			ret.put(Constants.VOUCHER_TOTAL, consumed);
		}catch(NullPointerException nx) {
			logger.error(nx.getMessage());
			nx.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}

		
		
		getTotalVoucherRemaining(filterMap, yyyymm, ret);
		logger.debug("{}", ret);
		
		return ret;
		
	}
	
	
	//For top level
	/**
	 * Total - with filter and dates and without filter and dates
	 */
	public void getTotalVoucherRemaining(Map<String, List<String>> filterMap, List<String> yyyymm, Map<String, Double> ret) {
		final String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;

		List<Bson> pipeLine = new ArrayList<>();

		if(filterMap!=null) {
			Bson q1QryMatch = common.formMatchClauseForListFilterBson(filterMap);
			pipeLine.add(q1QryMatch);
		}

		String qConvDate = "{$project:{"
				+ "_id: 0, "
				+ "ivu:'$invoiceUnitPriceAsPerTc', "
				+ "poN:'$purchaseOrderNumberOne', "
				+ "poQ: '$quantityOrderedPurchaseOrder', "
				+ "net: '$netPricePOPrice', "
				+ "priceUnitPo: '$priceUnitPo', "
				+ "idyy : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
				+ "podtyy : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }},"
				+ "vouch : '$voucherConsumed'"
		+ "} }";
		
		pipeLine.add(BasicDBObject.parse(qConvDate));			

		
		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("podtyy", yyyymm));
			pipeLine.add(dateFilter);
		}
		
		//now unwind
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeLine.add(Aggregates.unwind("$vouch"));

//		common.printDocs(collectionName, pipeLine, mongoTemplate);
		
		int cnt = common.getCount(collectionName, pipeLine, mongoTemplate);
		logger.debug("1 {}", cnt);
		
		//now getminimum
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeLine.add(BasicDBObject.parse("{$group:{_id:'$vouch.voucherId', rem:{'$min':'$vouch.remaining'}}}"));

//		common.printDocs(collectionName, pipeLine, mongoTemplate);
		cnt = common.getCount(collectionName, pipeLine, mongoTemplate);
		logger.debug("{}", cnt);
		
		//now Sum them up
		pipeLine.add(BasicDBObject.parse("{$group:{_id:null, rem:{'$sum':'$rem'}}}"));
		
		printDocs(collectionName, pipeLine);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> rec = mongo.getCollection(collectionName).aggregate(
				pipeLine
			)
		;
				
		try {
			double remaining = rec.first().getDouble("rem");
			ret.put(Constants.VOUCHER_REMAINING, remaining);
		}catch(NullPointerException nx) {
			logger.error(nx.getMessage());
			nx.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		logger.debug("{}", ret);
				
	}

	
	//get for last 7 days
	public Map<String, Map<String, Double>> getTotalVoucherKPIsForLast7Days(Map<String, List<String>> filterMap) {
		final String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;
		Map<String, Map<String, Double>> ret = new HashMap<>();
		
		List<Bson> pipeLine = new ArrayList<>();

		if(filterMap!=null) {
			Bson q1QryMatch = common.formMatchClauseForListFilterBson(filterMap);
			pipeLine.add(q1QryMatch);
		}
		
		LocalDate endDate = LocalDate.now();
		LocalDate startDate = LocalDate.now().minusDays(6);

		Bson q1MatchQry = match(and(gte("purchaseOrderCreationDate", startDate), lte("purchaseOrderCreationDate", endDate)));
		pipeLine.add(q1MatchQry);

		
		String qConvDate = "{$project:{"
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
		+ "} }";
		
		pipeLine.add(BasicDBObject.parse(qConvDate));			
		
//		//now unwind
		pipeLine.add(Aggregates.unwind("$vouch"));


		
		//now Sum them up
		pipeLine.add(BasicDBObject.parse("{$group:{_id:'$podtdd', con:{'$sum':'$vouch.consumed'}, rem:{'$sum':'$vouch.remaining'}}}"));
		
		pipeLine.add(Aggregates.sort(Sorts.ascending("_id")));
//		printDocs(collectionName, pipeLine);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> rec = mongo.getCollection(collectionName).aggregate(
				pipeLine
			)
		;
		rec.cursor().forEachRemaining(doc->{
			String dt = doc.getString("_id");
			Map<String, Double> tmp = new HashMap<>();
			double con = doc.getDouble("con");
			double rem = doc.getDouble("rem");
			tmp.put(Constants.VOUCHER_CONSUMED, con);
			tmp.put(Constants.VOUCHER_REMAINING, rem);
			ret.put(dt, tmp);
		});
		logger.debug("{}", ret);
		
		return ret;
		
	}

	
	
	private void printDocs(String collection, List<Bson> pipeline) {
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug(">>>>>>>>>>{}", doc);
		});

	}
	
}
