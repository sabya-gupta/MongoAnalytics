package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.in;

import java.time.LocalDate;
import java.time.YearMonth;
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
import org.springframework.stereotype.Service;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;

@Service
public class TopLevelAnalysisForLeakageRecoved {

	@Autowired
	Common common;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	MongoTemplate mongoTemplate;

	public double getTotalRecoveredValue(final Map<String, List<String>> argfilter, final List<String> yyyymm) {

		final String collectionName = Constants.LEAKAGE_RECOVERED_COLLECTION_NAME;
		final List<Bson> pipeline = new ArrayList<>();

		pipeline.add(BasicDBObject.parse("{$unwind : '$materialGroupL4'},"));

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
				"" + "{$project:{" + "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName',"
						+ "lvl1: '$valueLeakageCalcAsPerPeriod'" + "}}");
		pipeline.add(firstProjectVLBson);

		pipeline.add(BasicDBObject.parse("{$unwind : '$lvl1'},"));

		final Bson secondProjectVLBson = BasicDBObject.parse(
				"{$project:{" + "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "date:'$lvl1.yyyyDashMonth',"
						+ "vl: '$lvl1.valueLeakageIdentified',"
						+ "lastarr:{$slice:['$lvl1.amountAgreedSettledUserDetails', -1]}" + "} }");
		pipeline.add(secondProjectVLBson);


		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("date", yyyymm));
			pipeline.add(dateFilter);
		}

		pipeline.add(Aggregates.unwind("$lastarr"));

		final Bson lastValue = BasicDBObject.parse(
				"{$project:{" + "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "vl: 1,"
						+ "val:'$lastarr.valueLeakageAmountSettled'" + "} }");
		pipeline.add(lastValue);

		common.printDocs(collectionName, pipeline, mongoTemplate);
		final String projectLV = "{$group : {" + "_id: null, " + "val:{'$sum':'$val'} " + "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);

		common.printDocs(collectionName, pipeline, mongoTemplate);

//		int count = 0;
//		if (!second) {
//			count = common.getCount(collectionName, pipeline, mongoTemplate);
////			logger.debug("The total count should be {}", count);
//		}

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);
		double val = 0;

		try {
			val = ret.first().getDouble("val");
		} catch (final Exception e) {
			e.printStackTrace();
		}

		logger.debug("---> VT = {}", val);
		return val;

	}

	public Map<String, Double> getMONTH_WISERecoveredValueForLastONEYEAR(final Map<String, List<String>> argfilter) {

		final String collectionName = Constants.LEAKAGE_RECOVERED_COLLECTION_NAME;
		final List<Bson> pipeline = new ArrayList<>();
		pipeline.add(BasicDBObject.parse("{$unwind : '$materialGroupL4'},"));

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
				"" + "{$project:{" + "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName',"
						+ "lvl1: '$valueLeakageCalcAsPerPeriod'" + "}}");
		pipeline.add(firstProjectVLBson);

		pipeline.add(BasicDBObject.parse("{$unwind : '$lvl1'},"));

		final Bson secondProjectVLBson = BasicDBObject.parse(
				"{$project:{" + "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "date:'$lvl1.yyyyDashMonth',"
						+ "vl: '$lvl1.valueLeakageIdentified',"
						+ "lastarr:{$slice:['$lvl1.amountAgreedSettledUserDetails', -1]}" + "} }");
		pipeline.add(secondProjectVLBson);

		pipeline.add(Aggregates.unwind("$lastarr"));

		// add dateFilters
		final YearMonth ym = YearMonth.from(LocalDate.now());
		final List<String> yyyymm = new ArrayList<>();
		for (int i = 0; i < 12; i++) {
			yyyymm.add(ym.minusMonths(i).toString());
		}
		logger.debug("yyyymmmm {}", yyyymm);
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("date", yyyymm));
			pipeline.add(dateFilter);
		}

		final Bson lastValue = BasicDBObject.parse(
				"{$project:{" + "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "date: 1, "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "vl: 1,"
						+ "val:'$lastarr.valueLeakageAmountSettled'" + "} }");
		pipeline.add(lastValue);
//		common.printDocs(collectionName, pipeline, mongoTemplate);


//		common.printDocs(collectionName, pipeline, mongoTemplate);
		final String projectLV = "{$group : {" + "_id: '$date', " + "val:{'$sum':'$val'} " + "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);
		
		pipeline.add(Aggregates.sort(Sorts.ascending("_id")));

//		common.printDocs(collectionName, pipeline, mongoTemplate);

//		int count = 0;
//		if (!second) {
//			count = common.getCount(collectionName, pipeline, mongoTemplate);
////			logger.debug("The total count should be {}", count);
//		}

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		final Map<String, Double> retMap = new LinkedHashMap<>();
		
		ret.cursor().forEachRemaining(doc->{
			retMap.put(doc.getString("_id"), doc.getDouble("val"));
		});

		logger.debug("---> VT YW = {}", retMap);
		return retMap;

	}

}
