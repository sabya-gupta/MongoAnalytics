package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.regex;

import java.util.ArrayList;
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
import org.springframework.stereotype.Service;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

@Service
public class ValueLeakageAnalysis {

	@Autowired
	Common common;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	MongoTemplate mongoTemplate;

	public List<Map<String, String>> renderValueLeakageGridNew(final Map<String, List<String>> argfilter,
			final List<String> yyyymm, final String sortByField,
			final int orderByDirection, final int pgNum, final int pgSz, final List<List<Double>> valRangeFilters, final String searchField, final String searchStr) {

		final String collectionName = Constants.LEAKAGE_RECOVERED_COLLECTION_NAME;
		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		final Bson firstProjectVLBson = BasicDBObject.parse(
				"{$project:{"
				+ "tradingModel:1,"
				+ "priceAgreementReferenceName:1,"
				+ "supplierName:1,  "
				+ "opcoCode:1, "
				+ "outlineAgreementNumber:1, "
						+ "lvl1:'$valueLeakageCalcAsPerPeriod', " + "categoryManager : 1"
				+ "}}");
		pipeline.add(firstProjectVLBson);

		pipeline.add(Aggregates.unwind("$lvl1"));

		final Bson secondProjectVLBson = BasicDBObject.parse(
				"{$project:{" 
				+ "tradingModel:1,"
				+ "priceAgreementReferenceName:1,"
				+ "supplierName:1,  "
				+ "opcoCode:1, "
				+ "outlineAgreementNumber:1, "
				+ "reportPeriod:'$lvl1.yyyyDashMonth',"
			    + "lkgVal: '$lvl1.valueLeakageIdentified', "
						+ "lastarr:{$slice:['$lvl1.amountAgreedSettledUserDetails', -1]}," + "categoryManager : 1"
			    + "} }");
		pipeline.add(secondProjectVLBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}
		
		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(searchField, searchStr, "i"));
			pipeline.add(srchBson);
		}

		pipeline.add(Aggregates.unwind("$lastarr"));


		final String thirdProj =
				"{$project:{" 
				+ "tradingModel:1,"
				+ "priceAgreementReferenceName:1,"
				+ "supplierName:1,  "
				+ "opcoCode:1, "
				+ "outlineAgreementNumber:1, "
						+ Constants.LEAKAGE_VALUE + " : '$lkgVal', "
				+ "reportPeriod:1,"
						+ "categoryManager : 1,"
			    + "amountRecovered:'$lastarr.valueLeakageAmountSettled'"
			    + "} }";

				pipeline.add(BasicDBObject.parse(thirdProj));
//				common.printDocs(collectionName, pipeline, mongoTemplate);

//		int count = 0;
//		if (!second) {
//			count = common.getCount(collectionName, pipeline, mongoTemplate);
////			logger.debug("The total count should be {}", count);
//		}

//		final String projectedFinal = "{$project:{" + "tradingModel: '$_id.tradingModel', "
//				+ "priceAgreementReferenceName : '$_id.priceAgreementReferenceName', "
//				+ "supplierName : '$_id.supplierName', " + "categoryManager: '$_id.categoryManager', "
//				+ "opcoCode: '$_id.opcoCode', " + "outlineAgreementNumber: '$_id.outlineAgreementNumber', "
//				+ "'Report Period' : '$_id.yyyymm' , " + Constants.LEAKAGE_VALUE + ":'$lkgVal'" + "}}";
//
//		pipeline.add(BasicDBObject.parse(projectedFinal));

		//apply the value filter
		if(valRangeFilters!=null && valRangeFilters.size()>0) {
			final List<Bson> valFilters = new ArrayList<>();
			for(final List<Double>vals : valRangeFilters) {
				if(vals.size()==1) {
					valFilters.add(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)));
				}else {
					valFilters.add(Filters.and(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)) , Filters.lte(Constants.LEAKAGE_VALUE, vals.get(1))));
				}
					
			}
			pipeline.add(Aggregates.match(Filters.or(valFilters)));
		}
		

		if (sortByField != null && orderByDirection == Constants.SORT_DIRECTION_ASCENDING) {
			pipeline.add(Aggregates.sort(Sorts.ascending(sortByField)));
		}

		if (sortByField != null && orderByDirection == Constants.SORT_DIRECTION_DESCENDING) {
			pipeline.add(Aggregates.sort(Sorts.descending(sortByField)));
		}

		if (pgNum >= 0) {
			pipeline.add(Aggregates.skip(pgNum * pgSz));

			pipeline.add(Aggregates.limit(pgSz));
		}


		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		final List<Map<String, String>> retList = new ArrayList<>();
		ret.cursor().forEachRemaining(doc -> {
			logger.debug("kkk{}", doc);
			final Map<String, String> tmpMap = new HashMap<>();
			tmpMap.put("tradingModel", doc.getString("tradingModel"));
			tmpMap.put("priceAgreementReferenceName", doc.getString("priceAgreementReferenceName"));
			tmpMap.put("supplierName", doc.getString("supplierName"));
			tmpMap.put("opco", doc.getString("opcoCode"));
			tmpMap.put("ola", doc.getString("outlineAgreementNumber"));
			tmpMap.put("reportPeriod", doc.getString("reportPeriod"));
			tmpMap.put(Constants.LEAKAGE_VALUE, doc.getDouble(Constants.LEAKAGE_VALUE).toString());
			logger.debug("1");
			tmpMap.put(Constants.RECOVERED_VALUE, doc.getDouble("amountRecovered").toString());
			logger.debug("2");
			tmpMap.put("vpcOwner", doc.getString("categoryManager"));
			retList.add(tmpMap);
		});

		logger.debug("{}", retList);
		return retList;
	}

	public int getCountOfValueLeakageGridNew(final Map<String, List<String>> argfilter, final List<String> yyyymm,
			final List<List<Double>> valRangeFilters, final String searchField, final String searchStr) {

		final String collectionName = Constants.LEAKAGE_RECOVERED_COLLECTION_NAME;
		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		final Bson firstProjectVLBson = BasicDBObject.parse(
				"{$project:{"
				+ "tradingModel:1,"
				+ "priceAgreementReferenceName:1,"
				+ "supplierName:1,  "
				+ "opcoCode:1, "
				+ "outlineAgreementNumber:1, "
						+ "lvl1:'$valueLeakageCalcAsPerPeriod', " + "categoryManager : 1"
				+ "}}");
		pipeline.add(firstProjectVLBson);

		pipeline.add(Aggregates.unwind("$lvl1"));

		final Bson secondProjectVLBson = BasicDBObject.parse(
				"{$project:{" 
				+ "tradingModel:1,"
				+ "priceAgreementReferenceName:1,"
				+ "supplierName:1,  "
				+ "opcoCode:1, "
				+ "outlineAgreementNumber:1, "
				+ "reportPeriod:'$lvl1.yyyyDashMonth',"
			    + "lkgVal: '$lvl1.valueLeakageIdentified', "
						+ "lastarr:{$slice:['$lvl1.amountAgreedSettledUserDetails', -1]}," + "categoryManager : 1"
			    + "} }");
		pipeline.add(secondProjectVLBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}
		
		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(searchField, searchStr, "i"));
			pipeline.add(srchBson);
		}

		pipeline.add(Aggregates.unwind("$lastarr"));


		final String thirdProj =
				"{$project:{" 
				+ "tradingModel:1,"
				+ "priceAgreementReferenceName:1,"
				+ "supplierName:1,  "
				+ "opcoCode:1, "
				+ "outlineAgreementNumber:1, "
						+ Constants.LEAKAGE_VALUE + " : '$lkgVal', "
				+ "reportPeriod:1,"
						+ "categoryManager : 1,"
			    + "amountRecovered:'$lastarr.valueLeakageAmountSettled'"
			    + "} }";

				pipeline.add(BasicDBObject.parse(thirdProj));
				common.printDocs(collectionName, pipeline, mongoTemplate);

		//apply the value filter
		if(valRangeFilters!=null && valRangeFilters.size()>0) {
			final List<Bson> valFilters = new ArrayList<>();
			for(final List<Double>vals : valRangeFilters) {
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

	public MonthlyValueLeakageEntity renderValueLeakageGrid(final Map<String, List<String>> argfilter,
			final List<String> yyyymm, final String sortByField, final int orderByDirection, final int pgNum,
			final int pgSz, final List<List<Double>> valRangeFilters, final String searchField,
			final String searchStr) {

		final String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
//		final String collectionName = "testcoll";
		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		final Bson firstProjectVLBson = BasicDBObject.parse("" + "{$project: {" + "tradingModel:1, "
				+ "priceAgreementReferenceName:1, " + "supplierName:1, " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "materialGroupL4:1, " + "categoryManager:1, " 
				+ "vl: {$slice:['$valueLeakages', 1]}" 
				+ "}}");
		pipeline.add(firstProjectVLBson);

		pipeline.add(Aggregates.unwind("$vl"));
		common.printDocs(collectionName, pipeline, mongoTemplate);

		final Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" + "tradingModel:1, "
				+ "priceAgreementReferenceName:1, " + "supplierName:1, " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "dtt:'$vl.calculationDate', " + "categoryManager:1, "
				+ "yyyymm: {$dateToString: { format: '%Y-%m', date: '$vl.calculationDate' }}, "
				+ "materialGroupL4 : 1, "
				+ "adjustedPrevValueLeakage : '$vl.adjustedAmountPrevPeriods', "
				+ "val: '$vl.leakageValue' " + "} }");
		pipeline.add(secondProjectVLBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(searchField, searchStr, "i"));
			pipeline.add(srchBson);
		}
		common.printDocs(collectionName, pipeline, mongoTemplate);

		final String grpByLV = "{$group:{_id: {" + "tradingModel: '$tradingModel', "
				+ "priceAgreementReferenceName : '$priceAgreementReferenceName', " + "supplierName : '$supplierName', "
				+ "categoryManager: '$categoryManager', " + "opcoCode: '$opcoCode', "
				+ "outlineAgreementNumber: '$outlineAgreementNumber', " + "yyyymm : '$yyyymm' , " + "} , "
				+ "lkgVal:{$sum:'$val'} " 
				+ "materialGroupL4:{$addToSet:'$materialGroupL4'} ,"
				+ "prevVals : {$push:'$adjustedPrevValueLeakage'}"
				+ "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));
		common.printDocs(collectionName, pipeline, mongoTemplate);

//		int count = 0;
//		if (!second) {
//			count = common.getCount(collectionName, pipeline, mongoTemplate);
////			logger.debug("The total count should be {}", count);
//		}

		final String projectedFinal = "{$project:{" + "tradingModel: '$_id.tradingModel', "
				+ "priceAgreementReferenceName : '$_id.priceAgreementReferenceName', "
				+ "supplierName : '$_id.supplierName', " + "categoryManager: '$_id.categoryManager', "
				+ "opcoCode: '$_id.opcoCode', " + "outlineAgreementNumber: '$_id.outlineAgreementNumber', "
				+ "materialGroupL4 : 1, "
				+ "prevVals : 1, "
				+ "'Report Period' : '$_id.yyyymm' , " + Constants.LEAKAGE_VALUE + ":'$lkgVal'" + "}}";

		pipeline.add(BasicDBObject.parse(projectedFinal));

		// apply the value filter
		if (valRangeFilters != null && valRangeFilters.size() > 0) {
			final List<Bson> valFilters = new ArrayList<>();
			for (final List<Double> vals : valRangeFilters) {
				if (vals.size() == 1) {
					valFilters.add(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)));
				} else {
					valFilters.add(Filters.and(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)),
							Filters.lte(Constants.LEAKAGE_VALUE, vals.get(1))));
				}

			}
			pipeline.add(Aggregates.match(Filters.or(valFilters)));
		}

		common.printDocs(collectionName, pipeline, mongoTemplate);

		if (sortByField != null && orderByDirection == Constants.SORT_DIRECTION_ASCENDING) {
			pipeline.add(Aggregates.sort(Sorts.ascending(sortByField)));
		}

		if (sortByField != null && orderByDirection == Constants.SORT_DIRECTION_DESCENDING) {
			pipeline.add(Aggregates.sort(Sorts.descending(sortByField)));
		}

		if (pgNum >= 0) {
			pipeline.add(Aggregates.skip(pgNum * pgSz));

			pipeline.add(Aggregates.limit(pgSz));
		}

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		final Map<String, Map<String, Double>> lkgMap = new TreeMap<>();

		final List<Map<String, String>> retList = new ArrayList<>();
		ret.cursor().forEachRemaining(doc -> {
			logger.debug("{} ", doc);
			final Map<String, String> tmpMap = new HashMap<>();
			tmpMap.put("tradingModel", doc.getString("tradingModel"));
			final String prn = doc.getString("priceAgreementReferenceName");
			tmpMap.put("priceAgreementReferenceName", prn);
			tmpMap.put("supplierName", doc.getString("supplierName"));
			tmpMap.put("opco", doc.getString("opcoCode"));
			tmpMap.put("ola", doc.getString("outlineAgreementNumber"));
			tmpMap.put("reportPeriod", doc.getString("Report Period"));
			tmpMap.put(Constants.LEAKAGE_VALUE, doc.getDouble(Constants.LEAKAGE_VALUE).toString());
			tmpMap.put("vpcOwner", doc.getString("categoryManager"));
			final List<String> g4List = doc.getList("materialGroupL4", String.class);
			final String strg4List = String.join(",", g4List);
			tmpMap.put(Constants.PROP_MATERIAL_GROUP_4, strg4List);
			retList.add(tmpMap);
			final List<Map<String, Double>> prevVals = (List<Map<String, Double>>) doc.get("prevVals");
			logger.debug("{}>>>>>>", prevVals);
			prevVals.forEach(map -> {
				final Map<String, Double> tMap = lkgMap.get(prn) != null ? lkgMap.get(prn) : new TreeMap<>();
				map.keySet().forEach(key -> {
					Double val = tMap.get(key);
					if (val == null)
						val = 0.0;
					val = val + map.get(key);
					tMap.put(key, val);
				});

				lkgMap.put(prn, tMap);

			});
		});

		logger.debug("lkg map = {}", lkgMap);

		logger.debug(">>{}", retList);

		final MonthlyValueLeakageEntity retEnt = new MonthlyValueLeakageEntity();
		retEnt.setLeakagaesForPrevMonths(lkgMap);
		retEnt.setPriceReferenceDetails(retList);

		return retEnt;
	}

	public int getCountOfValueLeakageGrid(final Map<String, List<String>> argfilter, final List<String> yyyymm,
			final List<List<Double>> valRangeFilters, final String searchField, final String searchStr) {

		final String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		final Bson firstProjectVLBson = BasicDBObject.parse("" + "{$project: {" + "tradingModel:1, "
				+ "priceAgreementReferenceName:1, " + "supplierName:1, " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "categoryManager:1, " + "vl: {$slice:['$valueLeakages', 1]}" + "}}");
		pipeline.add(firstProjectVLBson);

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		pipeline.add(Aggregates.unwind("$vl"));

		final Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" + "tradingModel:1, "
				+ "priceAgreementReferenceName:1, " + "supplierName:1, " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "dtt:'$vl.calculationDate', " + "categoryManager:1, "
				+ "yyyymm: {$dateToString: { format: '%Y-%m', date: '$vl.calculationDate' }}, "
				+ "val: '$vl.leakageValue' " + "} }");
		pipeline.add(secondProjectVLBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(searchField, searchStr, "i"));
			pipeline.add(srchBson);
		}

		final String grpByLV = "{$group:{_id: {" + "tradingModel: '$tradingModel', "
				+ "priceAgreementReferenceName : '$priceAgreementReferenceName', " + "supplierName : '$supplierName', "
				+ "categoryManager: '$categoryManager', " + "opcoCode: '$opcoCode', "
				+ "outlineAgreementNumber: '$outlineAgreementNumber', " + "yyyymm : '$yyyymm' , " + "} , "
				+ "lkgVal:{$sum:'$val'} " + "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		// apply the value filter
		if (valRangeFilters != null && valRangeFilters.size() > 0) {

			final String projectedFinal = "{$project:{" + "tradingModel: '$_id.tradingModel', "
					+ "priceAgreementReferenceName : '$_id.priceAgreementReferenceName', "
					+ "supplierName : '$_id.supplierName', " + "categoryManager: '$_id.categoryManager', "
					+ "opcoCode: '$_id.opcoCode', " + "outlineAgreementNumber: '$_id.outlineAgreementNumber', "
					+ "'Report Period' : '$_id.yyyymm' , " + Constants.LEAKAGE_VALUE + ":'$lkgVal'" + "}}";

			pipeline.add(BasicDBObject.parse(projectedFinal));

			final List<Bson> valFilters = new ArrayList<>();
			for (final List<Double> vals : valRangeFilters) {
				if (vals.size() == 1) {
					valFilters.add(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)));
				} else {
					valFilters.add(Filters.and(Filters.gte(Constants.LEAKAGE_VALUE, vals.get(0)),
							Filters.lte(Constants.LEAKAGE_VALUE, vals.get(1))));
				}

			}
			pipeline.add(Aggregates.match(Filters.or(valFilters)));
		}

		int count = 0;
		count = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);
		return count;

	}

	public Map<String, Double> getMonthWiseValueLeakageForAPriceRef(final String priceref) {

		final String collectionName = Constants.LEAKAGE_RECOVERED_COLLECTION_NAME;
		final List<Bson> pipeline = new ArrayList<>();

		pipeline.add(match(Filters.eq("priceAgreementReferenceName", priceref)));

		final Bson firstProjectVLBson = BasicDBObject.parse("{$project:{" + "tradingModel:1,"
				+ "priceAgreementReferenceName:1," + "supplierName:1,  " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "lvl1:'$valueLeakageCalcAsPerPeriod', " + "categoryManager : 1" + "}}");
		pipeline.add(firstProjectVLBson);

		pipeline.add(Aggregates.unwind("$lvl1"));

		final Bson secondProjectVLBson = BasicDBObject.parse("{$project:{" + "tradingModel:1,"
				+ "priceAgreementReferenceName:1," + "supplierName:1,  " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "reportPeriod:'$lvl1.yyyyDashMonth'," + "lkgVal: '$lvl1.valueLeakageIdentified', "
				+ "lastarr:{$slice:['$lvl1.amountAgreedSettledUserDetails', -1]}," + "categoryManager : 1" + "} }");
		pipeline.add(secondProjectVLBson);

		pipeline.add(Aggregates.unwind("$lastarr"));

		final String thirdProj = "{$project:{" + "tradingModel:1," + "priceAgreementReferenceName:1,"
				+ "supplierName:1,  " + "opcoCode:1, " + "outlineAgreementNumber:1, " + Constants.LEAKAGE_VALUE
				+ " : '$lkgVal', " + "reportPeriod:1," + "categoryManager : 1,"
				+ "amountRecovered:'$lastarr.valueLeakageAmountSettled'" + "} }";

		pipeline.add(BasicDBObject.parse(thirdProj));

		pipeline.add(Aggregates.sort(Sorts.ascending("reportPeriod")));


		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		final Map<String, Double> retMap = new HashMap<>();
		ret.cursor().forEachRemaining(doc -> {
			logger.debug("kkk{}", doc);
			retMap.put(doc.getString("reportPeriod"), doc.getDouble(Constants.LEAKAGE_VALUE));
		});

		double total = 0.0;

		for (final String key : retMap.keySet()) {
			total = total + retMap.get(key);
		}

		retMap.put("TOTAL", total);

		logger.debug("MWVL {}", retMap);
		return retMap;

	}

	public double getValueRECOVEREDForAPriceRefForAMonth(final String priceref, final String yyyymm) {

		final String collectionName = Constants.LEAKAGE_RECOVERED_COLLECTION_NAME;
		final List<Bson> pipeline = new ArrayList<>();

		pipeline.add(match(Filters.eq("priceAgreementReferenceName", priceref)));

		final Bson firstProjectVLBson = BasicDBObject.parse("{$project:{" + "tradingModel:1,"
				+ "priceAgreementReferenceName:1," + "supplierName:1,  " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "lvl1:'$valueLeakageCalcAsPerPeriod', " + "categoryManager : 1" + "}}");
		pipeline.add(firstProjectVLBson);

		pipeline.add(Aggregates.unwind("$lvl1"));

		final Bson secondProjectVLBson = BasicDBObject.parse("{$project:{" + "tradingModel:1,"
				+ "priceAgreementReferenceName:1," + "supplierName:1,  " + "opcoCode:1, " + "outlineAgreementNumber:1, "
				+ "reportPeriod:'$lvl1.yyyyDashMonth'," + "lkgVal: '$lvl1.valueLeakageIdentified', "
				+ "lastarr:{$slice:['$lvl1.amountAgreedSettledUserDetails', -1]}," + "categoryManager : 1" + "} }");
		pipeline.add(secondProjectVLBson);

		pipeline.add(Aggregates.unwind("$lastarr"));

		final String thirdProj = "{$project:{" + "tradingModel:1," + "priceAgreementReferenceName:1,"
				+ "supplierName:1,  " + "opcoCode:1, " + "outlineAgreementNumber:1, " + Constants.LEAKAGE_VALUE
				+ " : '$lkgVal', " + "reportPeriod:1," + "categoryManager : 1,"
				+ "amountRecovered:'$lastarr.valueLeakageAmountSettled'" + "} }";

		pipeline.add(BasicDBObject.parse(thirdProj));

		pipeline.add(Aggregates.match(Filters.eq("reportPeriod", yyyymm)));

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		double val = 0.0;
		try {
			val = ret.first().getDouble("amountRecovered");
		} catch (final NullPointerException e) {
			e.printStackTrace();
		}

		logger.debug("RECOVERED FOR {} is =  {}", yyyymm, val);
		return val;

	}

}
