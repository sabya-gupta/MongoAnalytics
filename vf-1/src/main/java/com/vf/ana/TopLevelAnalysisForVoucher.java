package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.in;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
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

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

@Repository
public class TopLevelAnalysisForVoucher {

	@Autowired
	Common common;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	MongoTemplate mongoTemplate;

//	@Autowired
//	KPIFilterAndGroupByHandler kPIFilterAndGroupByHandler;

	public double getTotalVoucherValue(final Map<String, List<String>> argfilter, final List<String> yyyymm) {

		final String collectionName = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

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

		final Bson firstProjectVLBson = BasicDBObject.parse("{$project:{" + "vouch : '$appliedVoucher'"
				+ "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "} }");
		pipeline.add(firstProjectVLBson);
//		common.printDocs(collectionName, pipeline, mongoTemplate);

//		//now unwind
		pipeline.add(Aggregates.unwind("$vouch"));
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final Bson expandVouch = BasicDBObject.parse("{$project:{" + "vouchstdt : '$vouch.startDate'"
				+ "vouchenddt : '$vouch.endDate'" + "vouchval : '$vouch.totalValue'" + "vouchid : '$vouch.voucherId'"
				+ "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "} }");
		pipeline.add(expandVouch);

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {

			final List<Bson> andList = new ArrayList<>();
			for (final String ym : yyyymm) {
				final LocalDate firstDay = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
				final LocalDate lastDay = YearMonth.from(firstDay).atEndOfMonth();
				andList.add(Filters.and(Filters.gte("vouchenddt", firstDay), Filters.lte("vouchstdt", lastDay)));
			}

			if (andList.size() > 0) {
				pipeline.add(match(and(andList)));
			}

		}

//		printDocs(collectionName, pipeline);

		final String grpDist = "{$group:{" + "_id:{dim: '$vouchid'}, " + "val :{'$max':'$vouchval'} " + "}}";
		final Bson groupdistinct = BasicDBObject.parse(grpDist);
		pipeline.add(groupdistinct);
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final String addUpStr = "{$group:{" + "_id:null, " + "val :{'$sum':'$val'} " + "}}";

		final Bson addUp = BasicDBObject.parse(addUpStr);
		pipeline.add(addUp);

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

//		printDocs(collectionName, pipeline);

		double val = 0;

		try {
			val = ret.first().getDouble("val");
		} catch (final Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.debug("---> VT = {}", val);
		return val;
	}

	// For top level
	/**
	 * Total - with filter and dates and without filter and dates
	 * 
	 * @return
	 */
	public double getTotalVoucherRemaining(final Map<String, List<String>> filterMap, final List<String> yyyymm) {
		final String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;

		final List<Bson> pipeLine = new ArrayList<>();

		if (filterMap != null) {
			final Bson q1QryMatch = common.formMatchClauseForListFilterBson(filterMap);
			pipeLine.add(q1QryMatch);
		}

		final String qConvDate = "{$project:{" + "_id: 0, " + "ivu:'$invoiceUnitPriceAsPerTc', "
				+ "poN:'$purchaseOrderNumberOne', " + "poQ: '$quantityOrderedPurchaseOrder', "
				+ "net: '$netPricePOPrice', " + "priceUnitPo: '$priceUnitPo', "
				+ "idyy : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
				+ "podtyy : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }},"
				+ "vouch : '$voucherConsumed'" + "} }";

		pipeLine.add(BasicDBObject.parse(qConvDate));

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("podtyy", yyyymm));
			pipeLine.add(dateFilter);
		}

		// now unwind
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeLine.add(Aggregates.unwind("$vouch"));

//		common.printDocs(collectionName, pipeLine, mongoTemplate);

		int cnt = common.getCount(collectionName, pipeLine, mongoTemplate);
		logger.debug("1 {}", cnt);

		// now getminimum
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeLine.add(BasicDBObject.parse("{$group:{_id:'$vouch.voucherId', rem:{'$min':'$vouch.remaining'}}}"));

//		common.printDocs(collectionName, pipeLine, mongoTemplate);
		cnt = common.getCount(collectionName, pipeLine, mongoTemplate);
		logger.debug("{}", cnt);

		// now Sum them up
		pipeLine.add(BasicDBObject.parse("{$group:{_id:null, rem:{'$sum':'$rem'}}}"));

		common.printDocs(collectionName, pipeLine, mongoTemplate);

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> rec = mongo.getCollection(collectionName).aggregate(pipeLine);

		double ret = 0.0;
		try {
			ret = rec.first().getDouble("rem");
		} catch (final NullPointerException nx) {
			logger.error(nx.getMessage());
			nx.printStackTrace();
		} catch (final Exception e) {
			e.printStackTrace();
		}

		logger.debug("VR ==== {}", ret);
		return ret;

	}

	public Map<String, Double> getTotalVoucherRemainingValueForLast7Days(final Map<String, List<String>> filterMap) {

		final String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;

		final List<Bson> pipeLine = new ArrayList<>();

		if (filterMap != null) {
			final Bson q1QryMatch = common.formMatchClauseForListFilterBson(filterMap);
			pipeLine.add(q1QryMatch);
		}

		final String qConvDate = "{$project:{"
				+ "idyy : {$dateToString: { format: '%Y-%m-%d', date: '$invoiceDate' }}, "
				+ "podtyy : {$dateToString: { format: '%Y-%m-%d', date: '$purchaseOrderCreationDate' }},"
				+ "purchaseOrderCreationDate:1, " + "vouch : '$voucherConsumed'" + "} }";

		pipeLine.add(BasicDBObject.parse(qConvDate));

		// add dateFilters

		final LocalDate endDate = LocalDate.now();
//	final LocalDate endDate = LocalDate.parse("2020-02-10");
		logger.debug("day = {}", endDate);

		final LocalDate startDate = endDate.minusDays(6);
		logger.debug("day = {}", startDate);

		pipeLine.add(match(and(Filters.gte("purchaseOrderCreationDate", startDate),
				Filters.lte("purchaseOrderCreationDate", endDate))));

		// now unwind
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeLine.add(Aggregates.unwind("$vouch"));

		common.printDocs(collectionName, pipeLine, mongoTemplate);

//	int cnt = common.getCount(collectionName, pipeLine, mongoTemplate);
//	logger.debug("1 {}", cnt);

		// now getminimum
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		final String qry = "{$group:{_id:{podtyy:'$podtyy', vouchid: '$vouch.voucherId'}, rem:{'$min':'$vouch.remaining'}}}";
		pipeLine.add(BasicDBObject.parse(qry));

		common.printDocs(collectionName, pipeLine, mongoTemplate);
//	cnt = common.getCount(collectionName, pipeLine, mongoTemplate);
//	logger.debug("{}", cnt);

		// now Sum them up
		pipeLine.add(BasicDBObject.parse("{$group:{_id:'$_id.podtyy', rem:{'$sum':'$rem'}}}"));
		pipeLine.add(Aggregates.sort(Sorts.ascending("_id")));

		common.printDocs(collectionName, pipeLine, mongoTemplate);

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> rec = mongo.getCollection(collectionName).aggregate(pipeLine);

		final Map<String, Double> retMap = new LinkedHashMap<>();

		try {
			rec.cursor().forEachRemaining(doc -> {
				retMap.put(doc.getString("_id"), doc.getDouble("rem"));
			});
		} catch (final NullPointerException nx) {
			logger.error(nx.getMessage());
			nx.printStackTrace();
		} catch (final Exception e) {
			e.printStackTrace();
		}

		logger.debug("VR ==== {}", retMap);
		return retMap;

	}

	public Map<String, Double> getTotalVoucherRemainingValueForLastONEYEAR(final Map<String, List<String>> filterMap) {

		final String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;

		final List<Bson> pipeLine = new ArrayList<>();

		if (filterMap != null) {
			final Bson q1QryMatch = common.formMatchClauseForListFilterBson(filterMap);
			pipeLine.add(q1QryMatch);
		}

		final String qConvDate = "{$project:{" + "idyy : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
				+ "podtyy : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }},"
				+ "purchaseOrderCreationDate:1, " + "vouch : '$voucherConsumed'" + "} }";

		pipeLine.add(BasicDBObject.parse(qConvDate));

		// add dateFilters

		final LocalDate endDate = LocalDate.now();
//	final LocalDate endDate = LocalDate.parse("2020-02-10");
		logger.debug("day = {}", endDate);

		final LocalDate startDate = endDate.minusDays(365);
		logger.debug("day = {}", startDate);

		pipeLine.add(match(and(Filters.gte("purchaseOrderCreationDate", startDate),
				Filters.lte("purchaseOrderCreationDate", endDate))));

		// now unwind
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeLine.add(Aggregates.unwind("$vouch"));

		common.printDocs(collectionName, pipeLine, mongoTemplate);

//	int cnt = common.getCount(collectionName, pipeLine, mongoTemplate);
//	logger.debug("1 {}", cnt);

		// now getminimum
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		final String qry = "{$group:{_id:{podtyy:'$podtyy', vouchid: '$vouch.voucherId'}, rem:{'$min':'$vouch.remaining'}}}";
		pipeLine.add(BasicDBObject.parse(qry));

		common.printDocs(collectionName, pipeLine, mongoTemplate);
//	cnt = common.getCount(collectionName, pipeLine, mongoTemplate);
//	logger.debug("{}", cnt);

		// now Sum them up
		pipeLine.add(BasicDBObject.parse("{$group:{_id:'$_id.podtyy', rem:{'$sum':'$rem'}}}"));
		pipeLine.add(Aggregates.sort(Sorts.ascending("_id")));

		common.printDocs(collectionName, pipeLine, mongoTemplate);

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> rec = mongo.getCollection(collectionName).aggregate(pipeLine);

		final Map<String, Double> retMap = new LinkedHashMap<>();

		try {
			rec.cursor().forEachRemaining(doc -> {
				retMap.put(doc.getString("_id"), doc.getDouble("rem"));
			});
		} catch (final NullPointerException nx) {
			logger.error(nx.getMessage());
			nx.printStackTrace();
		} catch (final Exception e) {
			e.printStackTrace();
		}

		logger.debug("VR ==== {}", retMap);
		return retMap;

	}

	public double getTotalVoucherValueForSpecificDate(final Map<String, List<String>> argfilter, final String date) {

		final String collectionName = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

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

		final Bson firstProjectVLBson = BasicDBObject.parse("{$project:{" + "vouch : '$appliedVoucher'"//
				+ "} }");
		pipeline.add(firstProjectVLBson);
//		common.printDocs(collectionName, pipeline, mongoTemplate);

//		//now unwind
		pipeline.add(Aggregates.unwind("$vouch"));
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final Bson expandVouch = BasicDBObject.parse("{$project:{" + "vouchstdt : '$vouch.startDate'"
				+ "vouchenddt : '$vouch.endDate'" + "vouchval : '$vouch.totalValue'" + "vouchid : '$vouch.voucherId'" //
				+ "} }");
		pipeline.add(expandVouch);

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final LocalDate dt = LocalDate.parse(date, DateTimeFormatter.ISO_DATE);
		final Bson dateFilter = and(Filters.lte("vouchstdt", dt), Filters.gte("vouchenddt", dt));
		pipeline.add(match(dateFilter));

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final String grpDist = "{$group:{" //
				+ "_id:{vouchid: '$vouchid'}, " //
				+ "val :{'$max':'$vouchval'} " //
				+ "}}";
		final Bson groupdistinct = BasicDBObject.parse(grpDist);
		pipeline.add(groupdistinct);
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final String addUpStr = "{$group:{" + "_id:null, " + "val :{'$sum':'$val'} " + "}}";

		final Bson addUp = BasicDBObject.parse(addUpStr);
		pipeline.add(addUp);

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

//		printDocs(collectionName, pipeline);

		double val = 0;

		try {
			val = ret.first().getDouble("val");
		} catch (final Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.debug("---> VT = {}", val);
		return val;

	}

	public double getTotalVoucherValueForSpecificMonth(final Map<String, List<String>> argfilter, final String yyyymm) {

		final String collectionName = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

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

		final Bson firstProjectVLBson = BasicDBObject.parse("{$project:{" + "vouch : '$appliedVoucher'"//
				+ "} }");
		pipeline.add(firstProjectVLBson);
//		common.printDocs(collectionName, pipeline, mongoTemplate);

//		//now unwind
		pipeline.add(Aggregates.unwind("$vouch"));
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final Bson expandVouch = BasicDBObject.parse("{$project:{" + "vouchstdt : '$vouch.startDate'"
				+ "vouchenddt : '$vouch.endDate'" + "vouchval : '$vouch.totalValue'" + "vouchid : '$vouch.voucherId'" //
				+ "} }");
		pipeline.add(expandVouch);

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final LocalDate startdt = LocalDate.parse(yyyymm + "-01", DateTimeFormatter.ISO_DATE);
		final LocalDate enddt = YearMonth.from(startdt).atEndOfMonth();
		logger.debug("Start = {}, end = {}", startdt, enddt);
		final Bson dateFilter = Filters.and(Filters.lte("vouchstdt", startdt), Filters.gte("vouchenddt", enddt));
		pipeline.add(match(dateFilter));

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final String grpDist = "{$group:{" //
				+ "_id:{vouchid: '$vouchid'}, " //
				+ "val :{'$max':'$vouchval'} " //
				+ "}}";
		final Bson groupdistinct = BasicDBObject.parse(grpDist);
		pipeline.add(groupdistinct);
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final String addUpStr = "{$group:{" + "_id:null, " + "val :{'$sum':'$val'} " + "}}";

		final Bson addUp = BasicDBObject.parse(addUpStr);
		pipeline.add(addUp);

//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

//		printDocs(collectionName, pipeline);

		double val = 0;

		try {
			val = ret.first().getDouble("val");
		} catch (final Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.debug("---> VT = {}", val);
		return val;

	}

	public Map<String, Double> getTotalVoucherValueForLast7Days(final Map<String, List<String>> argfilter) {

		final List<String> dates = new ArrayList<>();
		final LocalDate enddate = LocalDate.now();
		for (int i = 0; i < 7; i++) {
			dates.add(enddate.minusDays(i).format(DateTimeFormatter.ISO_DATE));
		}
		logger.debug("{}", dates);
		final Map<String, Double> ret = new LinkedHashMap<>();
		for (final String date : dates) {
			final double val = getTotalVoucherValueForSpecificDate(argfilter, date);
			ret.put(date, val);
		}

		logger.debug("map = {}", ret);

		return ret;

	}

	public Map<String, Double> getTotalVoucherValueForLastONEYEAR(final Map<String, List<String>> argfilter) {

		final LocalDate enddate = LocalDate.now();
		final YearMonth endYYYYMM = YearMonth.of(enddate.getYear(), enddate.getMonth());
		final Map<String, Double> ret = new LinkedHashMap<>();
		for(int i=0; i<12; i++) {
			final YearMonth yyyymm = endYYYYMM.minusMonths(i);
			final String yyyymmStr = yyyymm.toString();
			final double val = getTotalVoucherValueForSpecificMonth(argfilter, yyyymmStr);
			ret.put(yyyymmStr, val);
		}
		logger.debug("map = {}", ret);

		return ret;

	}

}
