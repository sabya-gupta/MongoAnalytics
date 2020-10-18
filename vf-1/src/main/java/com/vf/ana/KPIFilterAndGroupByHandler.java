package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

@Repository
public class KPIFilterAndGroupByHandler {

	@Autowired
	Common common;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	MongoTemplate mongoTemplate;
	

	private final String dateStr = "invoiceDateyymm : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
			+ "purchaseOrderCreationDateyymm : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }}";

	private final String firstProject = "{$project:{supplierPartNumber:1, " + "tradingModel:1, " + "supplierId: 1, "
			+ "outlineAgreementNumber: 1, " + "catalogueType:1, " + "opcoCode:1, " + "priceAgreementStatus:1, "
			+ "parentSupplierId:1, " + "validFromDate:1, " + "validToDate:1, " + "materialGroupL4:1, "
			+ "priceAgreementReferenceName:1,"
			+ "quantityOrderedPurchaseOrder:{ $ifNull: ['$quantityOrderedPurchaseOrder', 0 ] },"
			+ "priceUnitPo:{ $ifNull: ['$priceUnitPo', 1 ] }," + "netPricePOPrice:1,"
			+ "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 0 ] },"
			+ "invoiceQuantity: { $ifNull: ['$invoiceQuantity', 0 ] }," + "invoiceDate:1,"
			+ "purchaseOrderCreationDate:1," + "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 1 ] },"
			+ dateStr + "}}";

	String dateStr2 = "validFromDatemm : {$dateToString: { format: '%Y-%m', date: '$validFromDate' }}, "
			+ "validToDateyymm : {$dateToString: { format: '%Y-%m', date: '$validToDate' }}";

	private final String firstProject2 = "{$project:{supplierPartNumber:1, " + "tradingModel:1, " + "supplierId: 1, "
			+ "outlineAgreementNumber: 1, " + "catalogueType:1, " + "opcoCode:1, " + "priceAgreementStatus:1, "
			+ "parentSupplierId:1, " + "validFromDate:1, " + "validToDate:1, " + "materialGroupL4:1, "
			+ "priceAgreementReferenceName:1,"
			+ "quantityOrderedPurchaseOrder:{ $ifNull: ['$quantityOrderedPurchaseOrder', 0 ] },"
			+ "priceUnitPo:{ $ifNull: ['$priceUnitPo', 1 ] }," + "netPricePOPrice:1,"
			+ "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 0 ] },"
			+ "invoiceQuantity: { $ifNull: ['$invoiceQuantity', 0 ] }," + "invoiceDate:1,"
			+ "purchaseOrderCreationDate:1," + "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 1 ] },"
			+ dateStr2 + "}}";

	public Map<String, Map<String, Double>> getDataByProp(final String grpByPropName, final String orderByKPI, final int dir,
			final Map<String, List<String>> filter, final int pgNum, final List<String> dates, final String searchStr) {

		if (orderByKPI == null || orderByKPI.equalsIgnoreCase(Constants.KPI_ORDER_VALUE))
			return getTotalOrdersValue(grpByPropName, dir, filter, pgNum, dates, null, false, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_INVOICE_VALUE))
			return getTotalInvoiceValue(grpByPropName, dir, filter, pgNum, dates, null, false, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_ORDERS_ISSUED))
			return getTotalOrdersIssued(grpByPropName, dir, filter, pgNum, dates, null, false, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT))
			return getTotalActiveItems(grpByPropName, dir, filter, pgNum, dates, null, false, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT))
			return getTotalActivePAs(grpByPropName, dir, filter, pgNum, dates, null, false, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_VLIDENTIFIED_COUNT))
			return getTotalLeakageValue(grpByPropName, dir, filter, pgNum, dates, null, false, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_VV_VALUE))
			return getTotalVoucherValue(grpByPropName, dir, filter, pgNum, dates, null, false, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_VVREMAINING_VALUE))
			return getTotalVoucherREMAININGValue(grpByPropName, dir, filter, pgNum, dates, null, false, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_VLREC_VALUE))
			return getTotalLeakageRecoveredValue(grpByPropName, dir, filter, pgNum, dates, null, false, searchStr);

		return null;
	}

	public int getDataCOUNTByProp(final String grpByPropName, final String orderByKPI, final int dir, final Map<String, List<String>> filter,
			final List<String> dates, final String searchStr) {

		if (orderByKPI == null || orderByKPI.equalsIgnoreCase(Constants.KPI_ORDER_VALUE))
			return getTotalOrdersValueCount(grpByPropName, dir, filter, dates, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_INVOICE_VALUE))
			return getTotalInvoiceValueCount(grpByPropName, dir, filter, dates, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_ORDERS_ISSUED))
			return getTotalOrdersIssuedCount(grpByPropName, dir, filter, dates, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT))
			return getTotalActiveItemsCount(grpByPropName, dir, filter, dates, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT))
			return getTotalActivePAsCount(grpByPropName, dir, filter, dates, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_VLIDENTIFIED_COUNT))
			return getTotalLeakageValueCount(grpByPropName, dir, filter, dates, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_VV_VALUE))
			return getTotalVoucherItemsCOUNT(grpByPropName, filter, dates, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_VVREMAINING_VALUE))
			return getTotalVoucherREMAINING_COUNT(grpByPropName, filter, dates, searchStr);

		if (orderByKPI.equalsIgnoreCase(Constants.KPI_VLREC_VALUE))
			return getTotalLeakageRecoveredCOUNT(grpByPropName, filter, dates, searchStr);

		return 0;
	}

	public Map<String, Map<String, Double>> getTotalOrdersValue(final String groupByPropName, final int dir,
			final Map<String, List<String>> argfilter, final int pgNum, final List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, final boolean second, final String searchStr) {

		if (retMap == null) {
			retMap = new LinkedHashMap<>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		final List<Bson> pipeline = new ArrayList<>();

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		// add already selected filter
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);

		final Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		final String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "po:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}, "
				+ "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final String projectQIV = "{$group:{_id: '$dim' , " + Constants.ORDER_VALUE + ":{$sum:'$po'} " + "}}";

		final Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		int count = 0;
		if (!second) {
			count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
//			logger.debug("The total count should be {}", count);
		}

		final List<String> kpisV = new ArrayList<>();
		kpisV.add(Constants.ORDER_VALUE);
		final String sortByField = Constants.ORDER_VALUE;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
				sortByField, mongoTemplate);

		if (!second) {
//			getTotalOrdersValue(groupByPropName, dir,
//					argfilter, pgNum, yyyymm,
//					retMap, true);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);
			
			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);
//
			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageRecoveredValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

		}

		logger.debug("FINAL    FINAL   OV 3 {}", retMap);

		return retMap;
	}

	public int getTotalOrdersValueCount(final String groupByPropName, final int dir, final Map<String, List<String>> argfilter,
			final List<String> yyyymm, final String searchStr) {

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			for (final String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);

		final Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		final String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "po:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}, "
				+ "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final String projectQIV = "{$group:{_id: '$dim' , " + Constants.ORDER_VALUE + ":{$sum:'$po'} " + "}}";

		final Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);

		return count;

	}

	public Map<String, Map<String, Double>> getTotalInvoiceValue(final String groupByPropName, final int dir,
			final Map<String, List<String>> argfilter, final int pgNum, final List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, final boolean second, final String searchStr) {

		if (retMap == null) {
			retMap = new LinkedHashMap<>();
		}
		if (retMap.size() == 0 && second)
			return retMap;

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			for (final String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);
		// add already selected filter
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);// tackle wayward dates

		final Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("invoiceDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		final String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "iv:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']}," + "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final String projectQIV = "{$group:{_id: '$dim' , " + Constants.INVOICE_VALUE + ":{$sum:'$iv'} }}";

		final Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		if (!second) {
			final int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
			logger.debug("The total count should be {}", count);
		}

		final List<String> kpisV = new ArrayList<>();
		kpisV.add(Constants.INVOICE_VALUE);
		final String sortByField = Constants.INVOICE_VALUE;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
				sortByField, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

//			getTotalInvoiceValue(groupByPropName, dir,
//					argfilter, pgNum, yyyymm,
//					retMap, true);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageRecoveredValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

		}

		logger.debug("FINAL    FINAL   OV 2 {}", retMap);

		return retMap;
	}

	public int getTotalInvoiceValueCount(final String groupByPropName, final int dir, final Map<String, List<String>> argfilter,
			final List<String> yyyymm, final String searchStr) {

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			for (final String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);// tackle wayward dates

		final Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("invoiceDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		final String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "iv:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']}," + "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final String projectQIV = "{$group:{_id: '$dim' , " + Constants.INVOICE_VALUE + ":{$sum:'$iv'} }}";

		final Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);

		return count;

	}

	public Map<String, Map<String, Double>> getTotalOrdersIssued(final String groupByPropName, final int dir,
			final Map<String, List<String>> argfilter, final int pgNum, final List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, final boolean second, final String searchStr) {

		if (retMap == null) {
			retMap = new LinkedHashMap<>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			for (final String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);// tackle wayward dates

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		// add already selected filter
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);

		final String projectPOIss = "{$group:{_id:{dim: '$" + groupByPropName + "', po:'$purchaseOrderNumberOne' }, "
				+ Constants.NUMBER_OF_ORDERS + ":{$sum:1} }}";
		final Bson bPrjOIss = BasicDBObject.parse(projectPOIss);
		pipeline.add(bPrjOIss);

		final String projectQIss = "{$group:{_id: '$_id.dim' , " + Constants.NUMBER_OF_ORDERS + ":{$sum:1}}} ";

		System.out.println(projectQIss);
		final Bson bPrjPOIss = BasicDBObject.parse(projectQIss);
		pipeline.add(bPrjPOIss);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		if (!second) {
			final int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
			logger.debug("The total count should be {}", count);
		}

		final List<String> kpisIss = new ArrayList<>();
		kpisIss.add(Constants.NUMBER_OF_ORDERS);
		common.getResults(pipeline, dir, pgNum, retMap, kpisIss, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
				Constants.NUMBER_OF_ORDERS, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageRecoveredValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);


		}

		logger.debug("FINAL    FINAL   OISS 3 {}", retMap);
		return retMap;
	}

	public int getTotalOrdersIssuedCount(final String groupByPropName, final int dir, final Map<String, List<String>> argfilter,
			final List<String> yyyymm, final String searchStr) {

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			for (final String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);// tackle wayward dates

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final String projectPOIss = "{$group:{_id:{dim: '$" + groupByPropName + "', po:'$purchaseOrderNumberOne' }, "
				+ Constants.NUMBER_OF_ORDERS + ":{$sum:1} }}";
		final Bson bPrjOIss = BasicDBObject.parse(projectPOIss);
		pipeline.add(bPrjOIss);

		final String projectQIss = "{$group:{_id: '$_id.dim' , " + Constants.NUMBER_OF_ORDERS + ":{$sum:1}}} ";

		System.out.println(projectQIss);
		final Bson bPrjPOIss = BasicDBObject.parse(projectQIss);
		pipeline.add(bPrjPOIss);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		final int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);
		return count;

	}

	public Map<String, Map<String, Double>> getTotalActivePAs(final String groupByPropName, final int dir,
			final Map<String, List<String>> argfilter, final int pgNum, final List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, final boolean second, final String searchStr) {

		final String collection = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		if (retMap == null) {
			retMap = new LinkedHashMap<>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			for (final String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter == null) {

			filter = new HashMap<>();
		}
		List<String> activeClause = filter.get(Constants.PROP_PRICEAGG_STATUS);
		if (activeClause == null)
			activeClause = new ArrayList<>();
		if (!activeClause.contains("Active")) {
			activeClause.add("Active");
		}

		filter.put(Constants.PROP_PRICEAGG_STATUS, activeClause);

		// Add filter
		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		// Add date
		if (yyyymm != null && yyyymm.size() > 0) {
			final List<Bson> dateFilterList = new ArrayList<>();
			for (final String ym : yyyymm) {
				final LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
				final Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
				dateFilterList.add(df);
			}
			final Bson orDf = or(dateFilterList);
			pipeline.add(match(orDf));
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		// Project whatever required and converted date fields
		final Bson firstProjectBson = BasicDBObject.parse(firstProject2);
		pipeline.add(firstProjectBson);

		// add already selected items if there
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

//		printDocs(collection, pipeline);

		// get distinct prop-spn-opco
		final String projectQ = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber', oc:'$opcoCode' }, "
				+ Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1} }}";
		final Bson bPrj = BasicDBObject.parse(projectQ);
		pipeline.add(bPrj);

		// get distint spn-opco
		final String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1}}}";
		final Bson bagg = BasicDBObject.parse(aggStr);
		pipeline.add(bagg);

//		printDocs(collection, pipeline);
		int count = 0;
		if (!second) {
			count = common.getCount(collection, pipeline, mongoTemplate);
			logger.debug("The total count should be {}", count);
		}

		final List<String> kpisAPA = new ArrayList<>();
		kpisAPA.add(Constants.ACTIVE_PRICE_AGREEMENT);
		common.getResults(pipeline, dir, pgNum, retMap, kpisAPA, collection, Constants.ACTIVE_PRICE_AGREEMENT, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

//			getTotalActivePAs(groupByPropName, dir,
//					argfilter, pgNum, yyyymm,
//					retMap, true);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageRecoveredValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);


		}

		logger.debug("FINAL    FINAL   APA 3 {}", retMap);
		return retMap;

	}

	public int getTotalActivePAsCount(final String groupByPropName, final int dir, final Map<String, List<String>> argfilter,
			final List<String> yyyymm, final String searchStr) {

		final String collection = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			for (final String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter == null) {

			filter = new HashMap<>();
		}
		List<String> activeClause = filter.get(Constants.PROP_PRICEAGG_STATUS);
		if (activeClause == null)
			activeClause = new ArrayList<>();
		if (!activeClause.contains("Active")) {
			activeClause.add("Active");
		}

		filter.put(Constants.PROP_PRICEAGG_STATUS, activeClause);

		// Add filter
		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		// Add date
		if (yyyymm != null && yyyymm.size() > 0) {
			final List<Bson> dateFilterList = new ArrayList<>();
			for (final String ym : yyyymm) {
				final LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
				final Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
				dateFilterList.add(df);
			}
			final Bson orDf = or(dateFilterList);
			pipeline.add(match(orDf));
		}

		// Project whatever required and converted date fields
		final Bson firstProjectBson = BasicDBObject.parse(firstProject2);
		pipeline.add(firstProjectBson);

//		printDocs(collection, pipeline);

		// get distinct prop-spn-opco
		final String projectQ = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber', oc:'$opcoCode' }, "
				+ Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1} }}";
		final Bson bPrj = BasicDBObject.parse(projectQ);
		pipeline.add(bPrj);

		// get distint spn-opco
		final String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1}}}";
		final Bson bagg = BasicDBObject.parse(aggStr);
		pipeline.add(bagg);

//		printDocs(collection, pipeline);
		final int count = common.getCount(collection, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);
		return count;

	}

	public Map<String, Map<String, Double>> getTotalActiveItems(final String groupByPropName, final int dir,
			final Map<String, List<String>> argfilter, final int pgNum, final List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, final boolean second, final String searchStr) {

		final String collection = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		if (retMap == null) {
			retMap = new LinkedHashMap<>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			for (final String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter == null) {

			filter = new HashMap<>();
		}
		List<String> activeClause = filter.get(Constants.PROP_PRICEAGG_STATUS);
		if (activeClause == null)
			activeClause = new ArrayList<>();
		if (!activeClause.contains("Active")) {
			activeClause.add("Active");
		}

		filter.put(Constants.PROP_PRICEAGG_STATUS, activeClause);

		// Add filter
		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		// Add date
		if (yyyymm != null && yyyymm.size() > 0) {
			final List<Bson> dateFilterList = new ArrayList<>();
			for (final String ym : yyyymm) {
				final LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
				final Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
				dateFilterList.add(df);
			}
			final Bson orDf = or(dateFilterList);
			pipeline.add(match(orDf));
		}

		// Project whatever required and converted date fields
		final Bson firstProjectBson = BasicDBObject.parse(firstProject2);
		pipeline.add(firstProjectBson);

		// add already selected items if there
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

//		printDocs(collection, pipeline);

		// get distinct prop-spn
		final String projectQ = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber' }, "
				+ Constants.ACTIVE_ITEMS + ":{$sum:1} }}";
		final Bson bPrj = BasicDBObject.parse(projectQ);
		pipeline.add(bPrj);

		// get distint spn-opco
		final String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_ITEMS + ":{$sum:1}}}";
		final Bson bagg = BasicDBObject.parse(aggStr);
		pipeline.add(bagg);

//		printDocs(collection, pipeline);
		if (!second) {
			final int count = common.getCount(collection, pipeline, mongoTemplate);
			logger.debug("The total count should be {}", count);
		}

		final List<String> kpisAPA = new ArrayList<>();
		kpisAPA.add(Constants.ACTIVE_ITEMS);
		common.getResults(pipeline, dir, pgNum, retMap, kpisAPA, collection, Constants.ACTIVE_ITEMS, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

//			getTotalActiveItems(groupByPropName, dir,
//					argfilter, pgNum, yyyymm,
//					retMap, true);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageRecoveredValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

		}

		logger.debug("FINAL    FINAL   APA 3 {}", retMap);
		return retMap;

	}

	public int getTotalActiveItemsCount(final String groupByPropName, final int dir, final Map<String, List<String>> argfilter,
			final List<String> yyyymm, final String searchStr) {

		final String collection = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			for (final String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter == null) {

			filter = new HashMap<>();
		}
		List<String> activeClause = filter.get(Constants.PROP_PRICEAGG_STATUS);
		if (activeClause == null)
			activeClause = new ArrayList<>();
		if (!activeClause.contains("Active")) {
			activeClause.add("Active");
		}

		filter.put(Constants.PROP_PRICEAGG_STATUS, activeClause);

		// Add filter
		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		// Add date
		if (yyyymm != null && yyyymm.size() > 0) {
			final List<Bson> dateFilterList = new ArrayList<>();
			for (final String ym : yyyymm) {
				final LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
				final Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
				dateFilterList.add(df);
			}
			final Bson orDf = or(dateFilterList);
			pipeline.add(match(orDf));
		}

		// Project whatever required and converted date fields
		final Bson firstProjectBson = BasicDBObject.parse(firstProject2);
		pipeline.add(firstProjectBson);

		// get distinct prop-spn
		final String projectQ = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber' }, "
				+ Constants.ACTIVE_ITEMS + ":{$sum:1} }}";
		final Bson bPrj = BasicDBObject.parse(projectQ);
		pipeline.add(bPrj);

		// get distint spn-opco
		final String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_ITEMS + ":{$sum:1}}}";
		final Bson bagg = BasicDBObject.parse(aggStr);
		pipeline.add(bagg);

		final int count = common.getCount(collection, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);

		return count;

	}

	public void aggregateMonthWiseForLastOneYear(final String propName, final String propVal, final Map<String, List<String>> filters) {

		final Map<String, Map<String, Double>> retMap = new TreeMap<>();

		LocalDate endDate = LocalDate.now();
		final String strEndDate = endDate.format(DateTimeFormatter.ISO_DATE);
		final LocalDate startDate = endDate.minusDays(365);
		final String strStartDate = startDate.format(DateTimeFormatter.ISO_DATE);

		final List<String> months = new ArrayList<>();
		final List<LocalDate> lastDays = new ArrayList<>();

		while (endDate.compareTo(startDate) >= 0) {
			final String yymm = endDate.format(DateTimeFormatter.ofPattern("YYYY-MM"));
			if (propName.equalsIgnoreCase(Constants.PROP_SUPPLIER_PART_NUMBER)) {
				final LocalDate ld = YearMonth.from(endDate).atEndOfMonth();
//				logger.debug("{} - {} ", endDate , ld);
				if (!lastDays.contains(ld))
					lastDays.add(ld);
			}
			if (!months.contains(yymm)) {
				months.add(yymm);
			}
			endDate = endDate.minusDays(25);
		}

//		logger.debug("MONTHS -> {}", months);

//		if(propName.equalsIgnoreCase(Constants.PROP_SUPPLIER_PART_NUMBER)) {
//			logger.debug("LAST DAYS -> {}", lastDays);
//		}

		final List<Bson> pipeLine = new ArrayList<>();

		if (filters != null) {
			pipeLine.add(common.formMatchClauseForListFilterBson(filters));
		}

		String q1 = "{$match: {$and:[    {purchaseOrderCreationDate:{$gte:ISODate('" + strStartDate
				+ "')}}, {purchaseOrderCreationDate:{$lte:ISODate('" + strEndDate + "')}}, " + "{" + propName + ":'"
				+ propVal + "'}  ]}}";
		Bson bson1 = BasicDBObject.parse(q1);
		pipeLine.add(bson1);

		String q2 = "{$project:{podate:{$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }}, value:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}   }}";
		Bson bson2 = BasicDBObject.parse(q2);
		pipeLine.add(bson2);

		String q3 = "{$group:{_id:'$podate', value:{$sum:'$value'}}}";
		Bson bson3 = BasicDBObject.parse(q3);
		pipeLine.add(bson3);

		String q4 = "{$sort:{_id:1}}";
		Bson bson4 = BasicDBObject.parse(q4);
		pipeLine.add(bson4);

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME)
				.aggregate(pipeLine);

		ret.cursor().forEachRemaining(doc -> {
//			logger.debug("1111111111111{}", doc);
			final String key = doc.getString("_id");

			if (months.contains(key)) {
				final Map<String, Double> hm = new HashMap<>();
				hm.put(Constants.ORDER_VALUE, doc.getDouble("value"));
				hm.put(Constants.INVOICE_VALUE, 0.00);
				if (propName.equalsIgnoreCase(Constants.PROP_SUPPLIER_PART_NUMBER))
					hm.put(Constants.SPN_PRICE, 0.00);
				retMap.put(doc.getString("_id"), hm);
//				logger.debug("putting 1 {}", key);
			}

		});

		pipeLine.clear();

		if (filters != null) {
			pipeLine.add(common.formMatchClauseForListFilterBson(filters));
		}

		q1 = "{$match: {$and:[    {invoiceDate:{$gte:ISODate('" + strStartDate + "')}}, {invoiceDate:{$lte:ISODate('"
				+ strEndDate + "')}}, " + "{" + propName + ":'" + propVal + "'}  ]}}";
		bson1 = BasicDBObject.parse(q1);
		pipeLine.add(bson1);

		q2 = "{$project:{podate:{$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, value:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']}   }}";
		bson2 = BasicDBObject.parse(q2);
		pipeLine.add(bson2);

		q3 = "{$group:{_id:'$podate', value:{$sum:'$value'}}}";
		bson3 = BasicDBObject.parse(q3);
		pipeLine.add(bson3);

		q4 = "{$sort:{_id:1}}";
		bson4 = BasicDBObject.parse(q4);
		pipeLine.add(bson4);

		mongo = mongoTemplate.getDb();
		ret = mongo.getCollection(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME).aggregate(pipeLine);

		ret.cursor().forEachRemaining(doc -> {
			final String key = doc.getString("_id");
			if (months.contains(key)) {
				Map<String, Double> hm = retMap.get(key);
				if (hm == null) {
					hm = new HashMap<>();
					hm.put(Constants.ORDER_VALUE, 0.00);
					if (propName.equalsIgnoreCase(Constants.PROP_SUPPLIER_PART_NUMBER))
						hm.put(Constants.SPN_PRICE, 0.00);
				}
				hm.put(Constants.INVOICE_VALUE, doc.getDouble("value"));
//				logger.debug("putting 2 {}", doc.getString("_id"));
				retMap.put(key, hm);
			}

		});

		for (final String mn : months) {
			Map<String, Double> hm = retMap.get(mn);
			if (hm == null) {
				hm = new HashMap<>();
				hm.put(Constants.ORDER_VALUE, 0.00);
				hm.put(Constants.INVOICE_VALUE, 0.00);
				if (propName.equalsIgnoreCase(Constants.PROP_SUPPLIER_PART_NUMBER))
					hm.put(Constants.SPN_PRICE, 0.00);
			}
//			logger.debug("putting 3 {}", mn);
			retMap.put(mn, hm);
		}

//		logger.debug("AFTER ALL!!!! {} ", retMap);

		if (propName.equalsIgnoreCase(Constants.PROP_SUPPLIER_PART_NUMBER)) {
			for (final LocalDate dt : lastDays) {
				final double price = getPriceforSPNbyDate(dt, propName, propVal);
				final String mn = dt.format(DateTimeFormatter.ofPattern("YYYY-MM"));
				if (!months.contains(mn))
					continue;
//				logger.debug("getting for {} - {}", mn, dt);
				Map<String, Double> hm = retMap.get(mn);
				if (hm == null) {
					hm = new HashMap<>();
					hm.put(Constants.ORDER_VALUE, 0.00);
					hm.put(Constants.INVOICE_VALUE, 0.00);
				}
				hm.put(Constants.SPN_PRICE, price);
				retMap.put(mn, hm);
			}
		}

		logger.debug("{}", retMap);

	}

	public Map<String, Map<String, Double>> getTotalLeakageValue(final String groupByPropName, final int dir,
			final Map<String, List<String>> argfilter, final int pgNum, final List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, final boolean second, final String searchStr) {

//		String collectionName = "leakage";
		
		final String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
		if (retMap == null) {
			retMap = new LinkedHashMap<>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

//		printDocs(collectionName, pipeline);

//		setCommonDateFiltersForPOToLimitFutureDate(pipeline);

		final Bson firstProjectVLBson = BasicDBObject
				.parse("" + "{$project: {" + "supplierPartNumber: '$supplierPartNumber', "
						+ "purchaseOrderNumberOne:'$purchaseOrderNumberOne', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName',"
						+ "invoiceDate: '$invoiceDate' ," + "vl: {$slice:['$valueLeakages', 1]}}" + "}");
		pipeline.add(firstProjectVLBson);

//		printDocs(collectionName, pipeline);

		pipeline.add(BasicDBObject.parse("{$unwind: { path: '$vl', preserveNullAndEmptyArrays: true } }"));

//		printDocs(collectionName, pipeline);

		final Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" + "supplierPartNumber: '$supplierPartNumber', "
				+ "purchaseOrderNumberOne:'$purchaseOrderNumberOne', " + "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "invoiceDate: '$invoiceDate' ,"
				+ "dtt:'$vl.calculationDate', "
				+ "yyyymm: {$dateToString: { format: '%Y-%m', date: '$vl.calculationDate' }}, "
				+ "val: '$vl.leakageValue' " + "} }");
		pipeline.add(secondProjectVLBson);

//		printDocs(collectionName, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		// add already selected filter
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

		final String projectLV = "{$project : {" + "dim: '$" + groupByPropName + "', " + "lv:'$val', " + "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);

//		printDocs(collectionName, pipeline);

		final String grpByLV = "{$group:{_id: '$dim' , " + Constants.LEAKAGE_VALUE + ":{$sum:'$lv'} " + "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));

//		printDocs(collectionName, pipeline);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		int count = 0;
		if (!second) {
			count = common.getCount(collectionName, pipeline, mongoTemplate);
//			logger.debug("The total count should be {}", count);
		}

		final List<String> kpisV = new ArrayList<>();
		kpisV.add(Constants.LEAKAGE_VALUE);
		final String sortByField = Constants.LEAKAGE_VALUE;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, collectionName, sortByField, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);
			
			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageRecoveredValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);



		}

		logger.debug("FINAL    FINAL   LEAKAGE 3 {}", retMap);

		return retMap;
	}

	public int getTotalLeakageValueCount(final String groupByPropName, final int dir, final Map<String, List<String>> argfilter,
			final List<String> yyyymm, final String searchStr) {

//		String collectionName = "leakage";
		final String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

//	logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		final Bson firstProjectVLBson = BasicDBObject
				.parse("" + "{$project: {" + "supplierPartNumber: '$supplierPartNumber', "
						+ "purchaseOrderNumberOne:'$purchaseOrderNumberOne', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName',"
						+ "invoiceDate: '$invoiceDate' ," + "vl: {$slice:['$valueLeakages', 1]}}" + "}");
		pipeline.add(firstProjectVLBson);

//		printDocs(collectionName, pipeline);

		pipeline.add(BasicDBObject.parse("{$unwind: { path: '$vl', preserveNullAndEmptyArrays: true } }"));

//		printDocs(collectionName, pipeline);

		final Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" + "supplierPartNumber: '$supplierPartNumber', "
				+ "purchaseOrderNumberOne:'$purchaseOrderNumberOne', " + "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "invoiceDate: '$invoiceDate' ,"
				+ "dtt:'$vl.calculationDate', "
				+ "yyyymm: {$dateToString: { format: '%Y-%m', date: '$vl.calculationDate' }}, "
				+ "val: '$vl.leakageValue' " + "} }");
		pipeline.add(secondProjectVLBson);

//		printDocs(collectionName, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		final String projectLV = "{$project : {" + "dim: '$" + groupByPropName + "', " + "lv:'$val', " + "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);

		final String grpByLV = "{$group:{_id: '$dim' , " + Constants.LEAKAGE_VALUE + ":{$sum:'$lv'} " + "}}";
		pipeline.add(BasicDBObject.parse(grpByLV));

		int count = 0;
		count = common.getCount(collectionName, pipeline, mongoTemplate);
		return count;
	}

	
	public Map<String, Double> getTotalLeakageValueForLastOneYearMonthWise(final Map<String, List<String>> argfilter) {

//		String collectionName = "leakage";
		
		final String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
		final Map<String, Double> retMap = new LinkedHashMap<>();

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}


		
		final Bson firstProjectVLBson = BasicDBObject
				.parse("" + "{$project: {" + "supplierPartNumber: '$supplierPartNumber', "
						+ "purchaseOrderNumberOne:'$purchaseOrderNumberOne', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName',"
						+ "invoiceDate: '$invoiceDate' ," + "vl: {$slice:['$valueLeakages', 1]}}" + "}");
		pipeline.add(firstProjectVLBson);

//		printDocs(collectionName, pipeline);

		pipeline.add(BasicDBObject.parse("{$unwind: { path: '$vl', preserveNullAndEmptyArrays: true } }"));

//		printDocs(collectionName, pipeline);

		final Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" + "supplierPartNumber: '$supplierPartNumber', "
				+ "purchaseOrderNumberOne:'$purchaseOrderNumberOne', " + "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "invoiceDate: '$invoiceDate' ,"
				+ "dtt:'$vl.calculationDate', "
				+ "yyyymm: {$dateToString: { format: '%Y-%m', date: '$vl.calculationDate' }}, "
				+ "val: '$vl.leakageValue' " + "} }");
		pipeline.add(secondProjectVLBson);

		
		final LocalDate endDate = LocalDate.now();
		final LocalDate startDate = endDate.minusDays(364);
		
		match(and(gte("dtt", startDate), lte("dtt", endDate)));

		
		
//		printDocs(collectionName, pipeline);


		final String projectLV = "{$project : {dim: '$yyyymm', " + "lv:'$val', " + "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);

//		printDocs(collectionName, pipeline);

		final String grpByLV = "{$group:{_id: '$dim' , " + Constants.LEAKAGE_VALUE + ":{$sum:'$lv'} " + "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));

//		printDocs(collectionName, pipeline);

		pipeline.add(BasicDBObject.parse("{$sort:{'_id':1}}"));

//		printDocs(collectionName, pipeline);
		
		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		
		ret.cursor().forEachRemaining(doc -> {
//			logger.debug("!!!!>>>>>>>>>>{}", doc);
			final String key = doc.getString("_id");
			retMap.put(key, doc.getDouble(Constants.LEAKAGE_VALUE));
		});

		return retMap;
	}

	
	
	private double getPriceforSPNbyDate(final LocalDate date, final String propName, final String propValue) {
		MongoDatabase mongo = mongoTemplate.getDb();
		mongo = mongoTemplate.getDb();
		final FindIterable<Document> ret = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME)
				.find(and(eq(propName, propValue), gte(Constants.PROP_VALID_TO_DATE, date),
						lte(Constants.PROP_VALID_FROM_DATE, date)));
		double num = 0;
		try {
			final String strDouble = ret.first().getString(Constants.PROP_NET_PRICE);
			num = Double.parseDouble(strDouble);
		} catch (final NullPointerException e) {

		} catch (final Exception ex) {
			ex.printStackTrace();
		}
		return num;
	}

	public Map<String, Map<String, Double>> getTotalVoucherValue(final String groupByPropName, final int dir,
			final Map<String, List<String>> argfilter, final int pgNum, final List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, final boolean second, final String searchStr) {

		final String collectionName = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		if (retMap == null) {
			retMap = new LinkedHashMap<>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

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

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}
		
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}


		final Bson firstProjectVLBson = BasicDBObject
				.parse("{$project:{"
						+ "vouch : '$appliedVoucher'"
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
//		common.printDocs(collectionName, pipeline, mongoTemplate);

//		//now unwind
		pipeline.add(Aggregates.unwind("$vouch"));
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		
		final Bson expandVouch = BasicDBObject
				.parse("{$project:{"
						+ "vouchstdt : '$vouch.startDate'"
						+ "vouchenddt : '$vouch.endDate'"
						+ "vouchval : '$vouch.totalValue'"
						+ "vouchid : '$vouch.voucherId'"
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
		pipeline.add(expandVouch);

		
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			
			final List<Bson> andList = new ArrayList<>();
			for(final String ym : yyyymm) {
				final LocalDate firstDay = LocalDate.parse(ym+"-01", DateTimeFormatter.ISO_DATE);
				final String yymm = firstDay.format(DateTimeFormatter.ofPattern("YYYY-MM"));
				final LocalDate lastDay = YearMonth.from(firstDay).atEndOfMonth();
				andList.add(Filters.and(Filters.gte("vouchenddt", firstDay), Filters.lte("vouchstdt", lastDay)));
			}
			
			if(andList.size()>0) {
				pipeline.add(match(and(andList)));
			}
			
		}



//		printDocs(collectionName, pipeline);


		final String grpDist = "{$group:{"
				+ "_id:{dim : '$"+groupByPropName+"', vid: '$vouchid'}, "
				+"val :{'$max':'$vouchval'} "
				+ "}}";
		final Bson groupdistinct = BasicDBObject.parse(grpDist);
		pipeline.add(groupdistinct);
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final String addUpStr = "{$group:{"
				+ "_id:'$_id.dim', "
				+Constants.VOUCHER_TOTAL+":{'$sum':'$val'} "
				+ "}}";
		
		final Bson addUp = BasicDBObject.parse(addUpStr);
		pipeline.add(addUp);

//		common.printDocs(collectionName, pipeline, mongoTemplate);


		final List<String> kpisV = new ArrayList<>();
		kpisV.add(Constants.VOUCHER_TOTAL);
		final String sortByField = Constants.VOUCHER_TOTAL;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, collectionName, sortByField, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);
			
			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, second, searchStr);

			getTotalLeakageRecoveredValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);
		}

		logger.debug("FINAL    FINAL   LEAKAGE 3 {}", retMap);

		return retMap;
	}


	public Map<String, Map<String, Double>> getTotalVoucherREMAININGValue(final String groupByPropName, final int dir,
			final Map<String, List<String>> argfilter, final int pgNum, final List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, final boolean second, final String searchStr) {

		final String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;

		if (retMap == null) {
			retMap = new LinkedHashMap<>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

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

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}
		
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}


		final Bson firstProjectVLBson = BasicDBObject
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
			final Bson dateFilter = match(in("podtyy", yyyymm));
			pipeline.add(dateFilter);
		}

//		//now unwind
		pipeline.add(Aggregates.unwind("$vouch"));
		int cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("-->>{}", cnt);


//		printDocs(collectionName, pipeline);


		
		//now getminimum
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeline.add(BasicDBObject.parse("{$group:{_id:{"+groupByPropName+": '$"+groupByPropName+"',voucherid: '$vouch.voucherId'}, rem:{'$min':'$vouch.remaining'}}}"));

//		common.printDocs(collectionName, pipeline, mongoTemplate);
		cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("-->>{}", cnt);

		
		final Bson secondProjectVLBson = BasicDBObject.parse("{$group:{_id:'$_id."+groupByPropName+"', "+
		Constants.VOUCHER_REMAINING+":{'$sum':'$rem'}}}");
		pipeline.add(secondProjectVLBson);

		common.printDocs(collectionName, pipeline, mongoTemplate);
		cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("1 -> {}", cnt);

		final List<String> kpisV = new ArrayList<>();
		kpisV.add(Constants.VOUCHER_REMAINING);
		final String sortByField = Constants.VOUCHER_REMAINING;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, collectionName, sortByField, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageRecoveredValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

		}

		logger.debug("FINAL    FINAL   LEAKAGE 3 {}", retMap);

		return retMap;
	}

	
	public int getTotalVoucherREMAINING_COUNT(final String groupByPropName,
			final Map<String, List<String>> argfilter, final List<String> yyyymm, final String searchStr) {

		final String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;

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

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}
		

		final Bson firstProjectVLBson = BasicDBObject
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
			final Bson dateFilter = match(in("podtyy", yyyymm));
			pipeline.add(dateFilter);
		}

//		//now unwind
		pipeline.add(Aggregates.unwind("$vouch"));
		int cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("-->>{}", cnt);


//		printDocs(collectionName, pipeline);


		
		//now getminimum
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeline.add(BasicDBObject.parse("{$group:{_id:{"+groupByPropName+": '$"+groupByPropName+"',voucherid: '$vouch.voucherId'}, rem:{'$min':'$vouch.remaining'}}}"));

//		common.printDocs(collectionName, pipeline, mongoTemplate);
		cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("-->>{}", cnt);

		
		final Bson secondProjectVLBson = BasicDBObject.parse("{$group:{_id:'$_id."+groupByPropName+"', "+
		Constants.VOUCHER_REMAINING+":{'$sum':'$rem'}}}");
		pipeline.add(secondProjectVLBson);

		common.printDocs(collectionName, pipeline, mongoTemplate);
		cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("1 -> {}", cnt);
		
		return cnt;

	}
	
	
	
	public int getTotalVoucherItemsCOUNT(final String groupByPropName, final Map<String, List<String>> argfilter, 
			final List<String> yyyymm, final String searchStr) {

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

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}
		

		final Bson firstProjectVLBson = BasicDBObject
				.parse("{$project:{"
						+ "vouch : '$appliedVoucher'"
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
//		common.printDocs(collectionName, pipeline, mongoTemplate);

//		//now unwind
		pipeline.add(Aggregates.unwind("$vouch"));
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		
		final Bson expandVouch = BasicDBObject
				.parse("{$project:{"
						+ "vouchstdt : '$vouch.startDate'"
						+ "vouchenddt : '$vouch.endDate'"
						+ "vouchval : '$vouch.totalValue'"
						+ "vouchid : '$vouch.voucherId'"
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
		pipeline.add(expandVouch);

		
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			
			final List<Bson> andList = new ArrayList<>();
			for(final String ym : yyyymm) {
				final LocalDate firstDay = LocalDate.parse(ym+"-01", DateTimeFormatter.ISO_DATE);
				final String yymm = firstDay.format(DateTimeFormatter.ofPattern("YYYY-MM"));
				final LocalDate lastDay = YearMonth.from(firstDay).atEndOfMonth();
				andList.add(Filters.and(Filters.gte("vouchenddt", firstDay), Filters.lte("vouchstdt", lastDay)));
			}
			
			if(andList.size()>0) {
				pipeline.add(match(and(andList)));
			}
			
		}



//		printDocs(collectionName, pipeline);


		final String grpDist = "{$group:{"
				+ "_id:{dim : '$"+groupByPropName+"', vid: '$vouchid'}, "
				+"val :{'$max':'$vouchval'} "
				+ "}}";
		final Bson groupdistinct = BasicDBObject.parse(grpDist);
		pipeline.add(groupdistinct);
//		common.printDocs(collectionName, pipeline, mongoTemplate);


		int count = 0;
		count = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("The count VT = {}", count);
		return count;
		
	}

	
	public Map<String, Map<String, Double>> getTotalLeakageRecoveredValue(final String groupByPropName, final int dir,
			final Map<String, List<String>> argfilter, final int pgNum, final List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, final boolean second, final String searchStr) {

//		String collectionName = "leakage";
		
		final String collectionName = Constants.LEAKAGE_RECOVERED_COLLECTION_NAME;
		if (retMap == null) {
			retMap = new LinkedHashMap<>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

//		printDocs(collectionName, pipeline);

//		setCommonDateFiltersForPOToLimitFutureDate(pipeline);

		final Bson firstProjectVLBson = BasicDBObject
				.parse("" + "{$project:{"
						+ "supplierPartNumber: '$supplierPartNumber', "
						+ "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " 
						+ "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " 
						+ "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " 
						+ "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," 
						+ "lvl1: '$valueLeakageCalcAsPerPeriod'"
						+ "}}");
		pipeline.add(firstProjectVLBson);

//		printDocs(collectionName, pipeline);

		pipeline.add(BasicDBObject.parse("{$unwind : '$lvl1'},"));

//		printDocs(collectionName, pipeline);

		final Bson secondProjectVLBson = BasicDBObject.parse("{$project:{"
				+ "supplierPartNumber: '$supplierPartNumber', "
				+ "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " 
				+ "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " 
				+ "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " 
				+ "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," 
				+ "date:'$lvl1.yyyyDashMonth',"
				+ "vl: '$lvl1.valueLeakageIdentified',"
				+ "lastarr:{$slice:['$lvl1.amountAgreedSettledUserDetails', -1]}"
				+ "} }");
		pipeline.add(secondProjectVLBson);

//		printDocs(collectionName, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("date", yyyymm));
			pipeline.add(dateFilter);
		}

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		// add already selected filter
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

		pipeline.add(Aggregates.unwind("$lastarr"));

		final Bson lastValue = BasicDBObject.parse("{$project:{"
				+ "supplierPartNumber: '$supplierPartNumber', "
				+ "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " 
				+ "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " 
				+ "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " 
				+ "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," 
				+ "vl: 1,"
				+ "val:'$lastarr.valueLeakageAmountSettled'"
				+ "} }");
		pipeline.add(lastValue);

		

		final String projectLV = "{$project : {" + "dim: '$" + groupByPropName + "', " + "lv:'$val', " + "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);
//		common.printDocs(collectionName, pipeline, mongoTemplate);


		final String grpByLV = "{$group:{_id: '$dim' , " + Constants.RECOVERED_VALUE + ":{$sum:'$lv'} " + "}}";
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		pipeline.add(BasicDBObject.parse(grpByLV));

//		common.printDocs(collectionName, pipeline, mongoTemplate);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		int count = 0;
		if (!second) {
			count = common.getCount(collectionName, pipeline, mongoTemplate);
//			logger.debug("The total count should be {}", count);
		}


		final List<String> kpisV = new ArrayList<>();
		kpisV.add(Constants.RECOVERED_VALUE);
		final String sortByField = Constants.RECOVERED_VALUE;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, collectionName, sortByField, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

		}

		logger.debug("FINAL    FINAL   RECOVERED 3 {}", retMap);

		return retMap;
	}

	public int getTotalLeakageRecoveredCOUNT(final String groupByPropName, final Map<String, List<String>> argfilter,
			final List<String> yyyymm, final String searchStr) {

//		String collectionName = "leakage";

		final String collectionName = Constants.LEAKAGE_RECOVERED_COLLECTION_NAME;

		final List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<>();
			filter.putAll(argfilter);
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			final Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			final Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

//		printDocs(collectionName, pipeline);

//		setCommonDateFiltersForPOToLimitFutureDate(pipeline);

		final Bson firstProjectVLBson = BasicDBObject.parse(
				"" + "{$project:{" + "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName',"
						+ "lvl1: '$valueLeakageCalcAsPerPeriod'" + "}}");
		pipeline.add(firstProjectVLBson);

//		printDocs(collectionName, pipeline);

		pipeline.add(BasicDBObject.parse("{$unwind : '$lvl1'},"));

//		printDocs(collectionName, pipeline);

		final Bson secondProjectVLBson = BasicDBObject.parse(
				"{$project:{" + "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "date:'$lvl1.yyyyDashMonth',"
						+ "vl: '$lvl1.valueLeakageIdentified',"
						+ "lastarr:{$slice:['$lvl1.amountAgreedSettledUserDetails', -1]}" + "} }");
		pipeline.add(secondProjectVLBson);

//		printDocs(collectionName, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			final Bson dateFilter = match(in("date", yyyymm));
			pipeline.add(dateFilter);
		}

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		// add already selected filter

		pipeline.add(Aggregates.unwind("$lastarr"));

		final Bson lastValue = BasicDBObject.parse(
				"{$project:{" + "supplierPartNumber: '$supplierPartNumber', " + "tradingModel: '$tradingModel' , "
						+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
						+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
						+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
						+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "vl: 1,"
						+ "val:'$lastarr.valueLeakageAmountSettled'" + "} }");
		pipeline.add(lastValue);

		final String projectLV = "{$project : {" + "dim: '$" + groupByPropName + "', " + "lv:'$val', " + "}}";
		final Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		final String grpByLV = "{$group:{_id: '$dim' , " + Constants.RECOVERED_VALUE + ":{$sum:'$lv'} " + "}}";
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		pipeline.add(BasicDBObject.parse(grpByLV));

		common.printDocs(collectionName, pipeline, mongoTemplate);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		int count = 0;
		count = common.getCount(collectionName, pipeline, mongoTemplate);
		return count;

	}
	
	
}
