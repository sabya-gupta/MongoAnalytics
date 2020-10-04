package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.in;
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
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

@Service
public class ValueLeakageAnalysis {

	@Autowired
	Common common;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	MongoTemplate mongoTemplate;

	public List<Map<String, String>> renderValueLeakageGrid(Map<String, List<String>> argfilter, List<String> yyyymm, String sortByField,
			int orderByDirection, int pgNum, int pgSz, List<List<Double>> valRangeFilters, String searchField, String searchStr) {

		String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
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

		Bson firstProjectVLBson = BasicDBObject.parse("" + "{$project: {" 
				+ "tradingModel:1, "
				+ "priceAgreementReferenceName:1, " 
				+ "supplierName:1, " 
				+ "opcoCode:1, " 
				+ "outlineAgreementNumber:1, "
				+ "categoryManager:1, " 
				+ "vl: {$slice:['$valueLeakages', 1]}" + "}}");
		pipeline.add(firstProjectVLBson);

		pipeline.add(Aggregates.unwind("$vl"));

		Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" 
				+ "tradingModel:1, "
				+ "priceAgreementReferenceName:1, " 
				+ "supplierName:1, " 
				+ "opcoCode:1, " 
				+ "outlineAgreementNumber:1, "
				+ "dtt:'$vl.calculationDate', " + "categoryManager:1, "
				+ "yyyymm: {$dateToString: { format: '%Y-%m', date: '$vl.calculationDate' }}, "
				+ "val: '$vl.leakageValue' " + "} }");
		pipeline.add(secondProjectVLBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}
		
		if (searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(searchField, searchStr, "i"));
			pipeline.add(srchBson);
		}


		String grpByLV = "{$group:{_id: {" + "tradingModel: '$tradingModel', "
				+ "priceAgreementReferenceName : '$priceAgreementReferenceName', " + "supplierName : '$supplierName', "
				+ "categoryManager: '$categoryManager', " + "opcoCode: '$opcoCode', "
				+ "outlineAgreementNumber: '$outlineAgreementNumber', " + "yyyymm : '$yyyymm' , " + "} , "
				+ "lkgVal:{$sum:'$val'} " + "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));
//		common.printDocs(collectionName, pipeline, mongoTemplate);

//		int count = 0;
//		if (!second) {
//			count = common.getCount(collectionName, pipeline, mongoTemplate);
////			logger.debug("The total count should be {}", count);
//		}

		String projectedFinal = "{$project:{" + "tradingModel: '$_id.tradingModel', "
				+ "priceAgreementReferenceName : '$_id.priceAgreementReferenceName', "
				+ "supplierName : '$_id.supplierName', " + "categoryManager: '$_id.categoryManager', "
				+ "opcoCode: '$_id.opcoCode', " + "outlineAgreementNumber: '$_id.outlineAgreementNumber', "
				+ "'Report Period' : '$_id.yyyymm' , " + Constants.LEAKAGE_VALUE + ":'$lkgVal'" + "}}";

		pipeline.add(BasicDBObject.parse(projectedFinal));

		//apply the value filter
		if(valRangeFilters!=null && valRangeFilters.size()>0) {
			List<Bson> valFilters = new ArrayList<>();
			for(List<Double>vals : valRangeFilters) {
				if(vals.size()==1) {
					valFilters.add(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)));
				}else {
					valFilters.add(Filters.and(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)) , Filters.lte(Constants.LEAKAGE_VALUE, vals.get(1))));
				}
					
			}
			pipeline.add(Aggregates.match(Filters.or(valFilters)));
		}
		
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		if (sortByField != null && orderByDirection == Constants.SORT_DIRECTION_ASCENDING) {
			pipeline.add(Aggregates.sort(Sorts.ascending(sortByField)));
		}

		if (sortByField != null && orderByDirection == Constants.SORT_DIRECTION_DESCENDING) {
			pipeline.add(Aggregates.sort(Sorts.descending(sortByField)));
		}

		pipeline.add(Aggregates.skip(pgNum * pgSz));

		pipeline.add(Aggregates.limit(pgSz));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		List<Map<String, String>> retList = new ArrayList<>();
		ret.cursor().forEachRemaining(doc -> {
			Map<String, String> tmpMap = new HashMap<>();
			tmpMap.put("tradingModel", doc.getString("tradingModel"));
			tmpMap.put("priceAgreementReferenceName", doc.getString("priceAgreementReferenceName"));
			tmpMap.put("supplierName", doc.getString("supplierName"));
			tmpMap.put("opco", doc.getString("opcoCode"));
			tmpMap.put("ola", doc.getString("outlineAgreementNumber"));
			tmpMap.put("reportPeriod", doc.getString("Report Period"));
			tmpMap.put(Constants.LEAKAGE_VALUE, doc.getDouble(Constants.LEAKAGE_VALUE).toString());
			tmpMap.put("vpcOwner", doc.getString("categoryManager"));
			retList.add(tmpMap);
		});

		logger.debug("{}", retList);
		return retList;
	}

	public int getCountOfValueLeakageGrid(Map<String, List<String>> argfilter, List<String> yyyymm, 
			List<List<Double>> valRangeFilters, String searchField, String searchStr) {

		String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
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

		Bson firstProjectVLBson = BasicDBObject.parse("" + "{$project: {" + "tradingModel:1, "
				+ "priceAgreementReferenceName:1, " + "supplierName:1, " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "categoryManager:1, " + "vl: {$slice:['$valueLeakages', 1]}" + "}}");
		pipeline.add(firstProjectVLBson);

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		pipeline.add(Aggregates.unwind("$vl"));

		Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" + "tradingModel:1, "
				+ "priceAgreementReferenceName:1, " + "supplierName:1, " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "dtt:'$vl.calculationDate', " + "categoryManager:1, "
				+ "yyyymm: {$dateToString: { format: '%Y-%m', date: '$vl.calculationDate' }}, "
				+ "val: '$vl.leakageValue' " + "} }");
		pipeline.add(secondProjectVLBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(searchField, searchStr, "i"));
			pipeline.add(srchBson);
		}

		String grpByLV = "{$group:{_id: {" + "tradingModel: '$tradingModel', "
				+ "priceAgreementReferenceName : '$priceAgreementReferenceName', " + "supplierName : '$supplierName', "
				+ "categoryManager: '$categoryManager', " + "opcoCode: '$opcoCode', "
				+ "outlineAgreementNumber: '$outlineAgreementNumber', " + "yyyymm : '$yyyymm' , " + "} , "
				+ "lkgVal:{$sum:'$val'} " + "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		//apply the value filter
		if(valRangeFilters!=null && valRangeFilters.size()>0) {
			
			String projectedFinal = "{$project:{" + "tradingModel: '$_id.tradingModel', "
					+ "priceAgreementReferenceName : '$_id.priceAgreementReferenceName', "
					+ "supplierName : '$_id.supplierName', " + "categoryManager: '$_id.categoryManager', "
					+ "opcoCode: '$_id.opcoCode', " + "outlineAgreementNumber: '$_id.outlineAgreementNumber', "
					+ "'Report Period' : '$_id.yyyymm' , " + Constants.LEAKAGE_VALUE + ":'$lkgVal'" + "}}";

			pipeline.add(BasicDBObject.parse(projectedFinal));

			
			
			List<Bson> valFilters = new ArrayList<>();
			for(List<Double>vals : valRangeFilters) {
				if(vals.size()==1) {
					valFilters.add(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)));
				}else {
					valFilters.add(Filters.and(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)) , Filters.lte(Constants.LEAKAGE_VALUE, vals.get(1))));
				}
					
			}
			pipeline.add(Aggregates.match(Filters.or(valFilters)));
		}

		int count = 0;
		count = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);
		return count;

	}

}
