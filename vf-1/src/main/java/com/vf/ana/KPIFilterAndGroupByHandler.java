package com.vf.ana;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.eq;
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
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UnwindOptions;

@Repository
public class KPIFilterAndGroupByHandler {

	@Autowired
	Common common;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	MongoTemplate mongoTemplate;
	

	private String dateStr = "invoiceDateyymm : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
			+ "purchaseOrderCreationDateyymm : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }}";

	private String firstProject = "{$project:{supplierPartNumber:1, " + "tradingModel:1, " + "supplierId: 1, "
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

	private String firstProject2 = "{$project:{supplierPartNumber:1, " + "tradingModel:1, " + "supplierId: 1, "
			+ "outlineAgreementNumber: 1, " + "catalogueType:1, " + "opcoCode:1, " + "priceAgreementStatus:1, "
			+ "parentSupplierId:1, " + "validFromDate:1, " + "validToDate:1, " + "materialGroupL4:1, "
			+ "priceAgreementReferenceName:1,"
			+ "quantityOrderedPurchaseOrder:{ $ifNull: ['$quantityOrderedPurchaseOrder', 0 ] },"
			+ "priceUnitPo:{ $ifNull: ['$priceUnitPo', 1 ] }," + "netPricePOPrice:1,"
			+ "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 0 ] },"
			+ "invoiceQuantity: { $ifNull: ['$invoiceQuantity', 0 ] }," + "invoiceDate:1,"
			+ "purchaseOrderCreationDate:1," + "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 1 ] },"
			+ dateStr2 + "}}";

	public Map<String, Map<String, Double>> getDataByProp(String grpByPropName, String orderByKPI, int dir,
			Map<String, List<String>> filter, int pgNum, List<String> dates, String searchStr) {

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

		return null;
	}

	public int getDataCOUNTByProp(String grpByPropName, String orderByKPI, int dir, Map<String, List<String>> filter,
			List<String> dates, String searchStr) {

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

		return 0;
	}

	public Map<String, Map<String, Double>> getTotalOrdersValue(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second, String searchStr) {

		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		List<Bson> pipeline = new ArrayList<>();

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		// add already selected filter
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			filter.putAll(argfilter);
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);

		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "po:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}, "
				+ "}}";
		Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		String projectQIV = "{$group:{_id: '$dim' , " + Constants.ORDER_VALUE + ":{$sum:'$po'} " + "}}";

		Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		int count = 0;
		if (!second) {
			count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
//			logger.debug("The total count should be {}", count);
		}

		List<String> kpisV = new ArrayList<String>();
		kpisV.add(Constants.ORDER_VALUE);
		String sortByField = Constants.ORDER_VALUE;
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

		}

		logger.debug("FINAL    FINAL   OV 3 {}", retMap);

		return retMap;
	}

	public int getTotalOrdersValueCount(String groupByPropName, int dir, Map<String, List<String>> argfilter,
			List<String> yyyymm, String searchStr) {

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);

		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "po:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}, "
				+ "}}";
		Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		String projectQIV = "{$group:{_id: '$dim' , " + Constants.ORDER_VALUE + ":{$sum:'$po'} " + "}}";

		Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);

		return count;

	}

	public Map<String, Map<String, Double>> getTotalInvoiceValue(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second, String searchStr) {

		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}
		if (retMap.size() == 0 && second)
			return retMap;

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);
		// add already selected filter
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
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

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);// tackle wayward dates

		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("invoiceDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "iv:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']}," + "}}";
		Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		String projectQIV = "{$group:{_id: '$dim' , " + Constants.INVOICE_VALUE + ":{$sum:'$iv'} }}";

		Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		if (!second) {
			int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
			logger.debug("The total count should be {}", count);
		}

		List<String> kpisV = new ArrayList<String>();
		kpisV.add(Constants.INVOICE_VALUE);
		String sortByField = Constants.INVOICE_VALUE;
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

		}

		logger.debug("FINAL    FINAL   OV 2 {}", retMap);

		return retMap;
	}

	public int getTotalInvoiceValueCount(String groupByPropName, int dir, Map<String, List<String>> argfilter,
			List<String> yyyymm, String searchStr) {

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);// tackle wayward dates

		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("invoiceDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "iv:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']}," + "}}";
		Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		String projectQIV = "{$group:{_id: '$dim' , " + Constants.INVOICE_VALUE + ":{$sum:'$iv'} }}";

		Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);

		return count;

	}

	public Map<String, Map<String, Double>> getTotalOrdersIssued(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second, String searchStr) {

		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);// tackle wayward dates

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		// add already selected filter
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

//		common.printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);

		String projectPOIss = "{$group:{_id:{dim: '$" + groupByPropName + "', po:'$purchaseOrderNumberOne' }, "
				+ Constants.NUMBER_OF_ORDERS + ":{$sum:1} }}";
		Bson bPrjOIss = BasicDBObject.parse(projectPOIss);
		pipeline.add(bPrjOIss);

		String projectQIss = "{$group:{_id: '$_id.dim' , " + Constants.NUMBER_OF_ORDERS + ":{$sum:1}}} ";

		System.out.println(projectQIss);
		Bson bPrjPOIss = BasicDBObject.parse(projectQIss);
		pipeline.add(bPrjPOIss);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		if (!second) {
			int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
			logger.debug("The total count should be {}", count);
		}

		List<String> kpisIss = new ArrayList<String>();
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

		}

		logger.debug("FINAL    FINAL   OISS 3 {}", retMap);
		return retMap;
	}

	public int getTotalOrdersIssuedCount(String groupByPropName, int dir, Map<String, List<String>> argfilter,
			List<String> yyyymm, String searchStr) {

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		common.setCommonDateFiltersForPOToLimitFutureDate(pipeline);// tackle wayward dates

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		String projectPOIss = "{$group:{_id:{dim: '$" + groupByPropName + "', po:'$purchaseOrderNumberOne' }, "
				+ Constants.NUMBER_OF_ORDERS + ":{$sum:1} }}";
		Bson bPrjOIss = BasicDBObject.parse(projectPOIss);
		pipeline.add(bPrjOIss);

		String projectQIss = "{$group:{_id: '$_id.dim' , " + Constants.NUMBER_OF_ORDERS + ":{$sum:1}}} ";

		System.out.println(projectQIss);
		Bson bPrjPOIss = BasicDBObject.parse(projectQIss);
		pipeline.add(bPrjPOIss);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		int count = common.getCount(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);
		return count;

	}

	public Map<String, Map<String, Double>> getTotalActivePAs(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second, String searchStr) {

		String collection = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter == null) {

			filter = new HashMap<String, List<String>>();
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

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		// Add date
		if (yyyymm != null && yyyymm.size() > 0) {
			List<Bson> dateFilterList = new ArrayList<>();
			for (String ym : yyyymm) {
				LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
				Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
				dateFilterList.add(df);
			}
			Bson orDf = or(dateFilterList);
			pipeline.add(match(orDf));
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		// Project whatever required and converted date fields
		Bson firstProjectBson = BasicDBObject.parse(firstProject2);
		pipeline.add(firstProjectBson);

		// add already selected items if there
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

//		printDocs(collection, pipeline);

		// get distinct prop-spn-opco
		String projectQ = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber', oc:'$opcoCode' }, "
				+ Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1} }}";
		Bson bPrj = BasicDBObject.parse(projectQ);
		pipeline.add(bPrj);

		// get distint spn-opco
		String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1}}}";
		Bson bagg = BasicDBObject.parse(aggStr);
		pipeline.add(bagg);

//		printDocs(collection, pipeline);
		int count = 0;
		if (!second) {
			count = common.getCount(collection, pipeline, mongoTemplate);
			logger.debug("The total count should be {}", count);
		}

		List<String> kpisAPA = new ArrayList<String>();
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


		}

		logger.debug("FINAL    FINAL   APA 3 {}", retMap);
		return retMap;

	}

	public int getTotalActivePAsCount(String groupByPropName, int dir, Map<String, List<String>> argfilter,
			List<String> yyyymm, String searchStr) {

		String collection = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter == null) {

			filter = new HashMap<String, List<String>>();
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

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		// Add date
		if (yyyymm != null && yyyymm.size() > 0) {
			List<Bson> dateFilterList = new ArrayList<>();
			for (String ym : yyyymm) {
				LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
				Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
				dateFilterList.add(df);
			}
			Bson orDf = or(dateFilterList);
			pipeline.add(match(orDf));
		}

		// Project whatever required and converted date fields
		Bson firstProjectBson = BasicDBObject.parse(firstProject2);
		pipeline.add(firstProjectBson);

//		printDocs(collection, pipeline);

		// get distinct prop-spn-opco
		String projectQ = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber', oc:'$opcoCode' }, "
				+ Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1} }}";
		Bson bPrj = BasicDBObject.parse(projectQ);
		pipeline.add(bPrj);

		// get distint spn-opco
		String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1}}}";
		Bson bagg = BasicDBObject.parse(aggStr);
		pipeline.add(bagg);

//		printDocs(collection, pipeline);
		int count = common.getCount(collection, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);
		return count;

	}

	public Map<String, Map<String, Double>> getTotalActiveItems(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second, String searchStr) {

		String collection = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}

		if (retMap.size() == 0 && second)
			return retMap;

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter == null) {

			filter = new HashMap<String, List<String>>();
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

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		// Add date
		if (yyyymm != null && yyyymm.size() > 0) {
			List<Bson> dateFilterList = new ArrayList<>();
			for (String ym : yyyymm) {
				LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
				Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
				dateFilterList.add(df);
			}
			Bson orDf = or(dateFilterList);
			pipeline.add(match(orDf));
		}

		// Project whatever required and converted date fields
		Bson firstProjectBson = BasicDBObject.parse(firstProject2);
		pipeline.add(firstProjectBson);

		// add already selected items if there
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

//		printDocs(collection, pipeline);

		// get distinct prop-spn
		String projectQ = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber' }, "
				+ Constants.ACTIVE_ITEMS + ":{$sum:1} }}";
		Bson bPrj = BasicDBObject.parse(projectQ);
		pipeline.add(bPrj);

		// get distint spn-opco
		String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_ITEMS + ":{$sum:1}}}";
		Bson bagg = BasicDBObject.parse(aggStr);
		pipeline.add(bagg);

//		printDocs(collection, pipeline);
		if (!second) {
			int count = common.getCount(collection, pipeline, mongoTemplate);
			logger.debug("The total count should be {}", count);
		}

		List<String> kpisAPA = new ArrayList<String>();
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


		}

		logger.debug("FINAL    FINAL   APA 3 {}", retMap);
		return retMap;

	}

	public int getTotalActiveItemsCount(String groupByPropName, int dir, Map<String, List<String>> argfilter,
			List<String> yyyymm, String searchStr) {

		String collection = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter == null) {

			filter = new HashMap<String, List<String>>();
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

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

		// Add date
		if (yyyymm != null && yyyymm.size() > 0) {
			List<Bson> dateFilterList = new ArrayList<>();
			for (String ym : yyyymm) {
				LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
				Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
				dateFilterList.add(df);
			}
			Bson orDf = or(dateFilterList);
			pipeline.add(match(orDf));
		}

		// Project whatever required and converted date fields
		Bson firstProjectBson = BasicDBObject.parse(firstProject2);
		pipeline.add(firstProjectBson);

		// get distinct prop-spn
		String projectQ = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber' }, "
				+ Constants.ACTIVE_ITEMS + ":{$sum:1} }}";
		Bson bPrj = BasicDBObject.parse(projectQ);
		pipeline.add(bPrj);

		// get distint spn-opco
		String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_ITEMS + ":{$sum:1}}}";
		Bson bagg = BasicDBObject.parse(aggStr);
		pipeline.add(bagg);

		int count = common.getCount(collection, pipeline, mongoTemplate);
		logger.debug("The total count should be {}", count);

		return count;

	}

	public void aggregateMonthWiseForLastOneYear(String propName, String propVal, Map<String, List<String>> filters) {

		Map<String, Map<String, Double>> retMap = new TreeMap<>();

		LocalDate endDate = LocalDate.now();
		String strEndDate = endDate.format(DateTimeFormatter.ISO_DATE);
		LocalDate startDate = endDate.minusDays(365);
		String strStartDate = startDate.format(DateTimeFormatter.ISO_DATE);

		List<String> months = new ArrayList<String>();
		List<LocalDate> lastDays = new ArrayList<>();

		while (endDate.compareTo(startDate) >= 0) {
			String yymm = endDate.format(DateTimeFormatter.ofPattern("YYYY-MM"));
			if (propName.equalsIgnoreCase(Constants.PROP_SUPPLIER_PART_NUMBER)) {
				LocalDate ld = YearMonth.from(endDate).atEndOfMonth();
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

		List<Bson> pipeLine = new ArrayList<Bson>();

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
			String key = doc.getString("_id");

			if (months.contains(key)) {
				Map<String, Double> hm = new HashMap<>();
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
			String key = doc.getString("_id");
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

		for (String mn : months) {
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
			for (LocalDate dt : lastDays) {
				double price = getPriceforSPNbyDate(dt, propName, propVal);
				String mn = dt.format(DateTimeFormatter.ofPattern("YYYY-MM"));
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

	public Map<String, Map<String, Double>> getTotalLeakageValue(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second, String searchStr) {

//		String collectionName = "leakage";
		
		String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
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

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		if (!second && searchStr != null && searchStr.trim().length() != 0) {
			Bson srchBson = match(regex(groupByPropName, searchStr, "i"));
			pipeline.add(srchBson);
		}

//		printDocs(collectionName, pipeline);

//		setCommonDateFiltersForPOToLimitFutureDate(pipeline);

		Bson firstProjectVLBson = BasicDBObject
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

		Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" + "supplierPartNumber: '$supplierPartNumber', "
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
			Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		// add already selected filter
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

		String projectLV = "{$project : {" + "dim: '$" + groupByPropName + "', " + "lv:'$val', " + "}}";
		Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);

//		printDocs(collectionName, pipeline);

		String grpByLV = "{$group:{_id: '$dim' , " + Constants.LEAKAGE_VALUE + ":{$sum:'$lv'} " + "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));

//		printDocs(collectionName, pipeline);

//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		int count = 0;
		if (!second) {
			count = common.getCount(collectionName, pipeline, mongoTemplate);
//			logger.debug("The total count should be {}", count);
		}

		List<String> kpisV = new ArrayList<String>();
		kpisV.add(Constants.LEAKAGE_VALUE);
		String sortByField = Constants.LEAKAGE_VALUE;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, collectionName, sortByField, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);
			
			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);




		}

		logger.debug("FINAL    FINAL   LEAKAGE 3 {}", retMap);

		return retMap;
	}

	public int getTotalLeakageValueCount(String groupByPropName, int dir, Map<String, List<String>> argfilter,
			List<String> yyyymm, String searchStr) {

//		String collectionName = "leakage";
		String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			filter.putAll(argfilter);
		}

//	logger.debug("{} - {}", groupByPropName, orderByKPIField);

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

		Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" + "supplierPartNumber: '$supplierPartNumber', "
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
			Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		String projectLV = "{$project : {" + "dim: '$" + groupByPropName + "', " + "lv:'$val', " + "}}";
		Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);

		String grpByLV = "{$group:{_id: '$dim' , " + Constants.LEAKAGE_VALUE + ":{$sum:'$lv'} " + "}}";
		pipeline.add(BasicDBObject.parse(grpByLV));

		int count = 0;
		count = common.getCount(collectionName, pipeline, mongoTemplate);
		return count;
	}

	
	public Map<String, Double> getTotalLeakageValueForLastOneYearMonthWise(Map<String, List<String>> argfilter) {

//		String collectionName = "leakage";
		
		String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
		Map<String, Double> retMap = new LinkedHashMap<String, Double>();

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			filter.putAll(argfilter);
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}


		
		Bson firstProjectVLBson = BasicDBObject
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

		Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" + "supplierPartNumber: '$supplierPartNumber', "
				+ "purchaseOrderNumberOne:'$purchaseOrderNumberOne', " + "tradingModel: '$tradingModel' , "
				+ "supplierId: '$supplierId' , " + "outlineAgreementNumber: '$outlineAgreementNumber', "
				+ "catalogueType: '$catalogueType' , " + "opcoCode: '$opcoCode', "
				+ "parentSupplierId: '$parentSupplierId' , " + "materialGroupL4: '$materialGroupL4' , "
				+ "priceAgreementReferenceName: '$priceAgreementReferenceName'," + "invoiceDate: '$invoiceDate' ,"
				+ "dtt:'$vl.calculationDate', "
				+ "yyyymm: {$dateToString: { format: '%Y-%m', date: '$vl.calculationDate' }}, "
				+ "val: '$vl.leakageValue' " + "} }");
		pipeline.add(secondProjectVLBson);

		
		LocalDate endDate = LocalDate.now();
		LocalDate startDate = endDate.minusDays(364);
		
		match(and(gte("dtt", startDate), lte("dtt", endDate)));

		
		
//		printDocs(collectionName, pipeline);


		String projectLV = "{$project : {dim: '$yyyymm', " + "lv:'$val', " + "}}";
		Bson bPrjOIV = BasicDBObject.parse(projectLV);
		pipeline.add(bPrjOIV);

//		printDocs(collectionName, pipeline);

		String grpByLV = "{$group:{_id: '$dim' , " + Constants.LEAKAGE_VALUE + ":{$sum:'$lv'} " + "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));

//		printDocs(collectionName, pipeline);

		pipeline.add(BasicDBObject.parse("{$sort:{'_id':1}}"));

//		printDocs(collectionName, pipeline);
		
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		
		ret.cursor().forEachRemaining(doc -> {
//			logger.debug("!!!!>>>>>>>>>>{}", doc);
			String key = doc.getString("_id");
			retMap.put(key, doc.getDouble(Constants.LEAKAGE_VALUE));
		});

		return retMap;
	}

	
	
	private double getPriceforSPNbyDate(LocalDate date, String propName, String propValue) {
		MongoDatabase mongo = mongoTemplate.getDb();
		mongo = mongoTemplate.getDb();
		FindIterable<Document> ret = mongo.getCollection(Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME)
				.find(and(eq(propName, propValue), gte(Constants.PROP_VALID_TO_DATE, date),
						lte(Constants.PROP_VALID_FROM_DATE, date)));
		double num = 0;
		try {
			String strDouble = ret.first().getString(Constants.PROP_NET_PRICE);
			num = Double.parseDouble(strDouble);
		} catch (NullPointerException e) {

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return num;
	}

	public Map<String, Map<String, Double>> getTotalVoucherValue(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second, String searchStr) {

		String collectionName = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

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
		
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}


		Bson firstProjectVLBson = BasicDBObject
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

		
		Bson expandVouch = BasicDBObject
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
			
			List<Bson> andList = new ArrayList<>();
			for(String ym : yyyymm) {
				LocalDate firstDay = LocalDate.parse(ym+"-01", DateTimeFormatter.ISO_DATE);
				String yymm = firstDay.format(DateTimeFormatter.ofPattern("YYYY-MM"));
				LocalDate lastDay = YearMonth.from(firstDay).atEndOfMonth();
				andList.add(Filters.and(Filters.gte("vouchenddt", firstDay), Filters.lte("vouchstdt", lastDay)));
			}
			
			if(andList.size()>0) {
				pipeline.add(match(and(andList)));
			}
			
		}



//		printDocs(collectionName, pipeline);


		String grpDist = "{$group:{"
				+ "_id:{dim : '$"+groupByPropName+"', vid: '$vouchid'}, "
				+"val :{'$max':'$vouchval'} "
				+ "}}";
		Bson groupdistinct = BasicDBObject.parse(grpDist);
		pipeline.add(groupdistinct);
//		common.printDocs(collectionName, pipeline, mongoTemplate);

		String addUpStr = "{$group:{"
				+ "_id:'$_id.dim', "
				+Constants.VOUCHER_TOTAL+":{'$sum':'$val'} "
				+ "}}";
		
		Bson addUp = BasicDBObject.parse(addUpStr);
		pipeline.add(addUp);

//		common.printDocs(collectionName, pipeline, mongoTemplate);


		List<String> kpisV = new ArrayList<String>();
		kpisV.add(Constants.VOUCHER_TOTAL);
		String sortByField = Constants.VOUCHER_TOTAL;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, collectionName, sortByField, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);
			
			getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, second, searchStr);

		}

		logger.debug("FINAL    FINAL   LEAKAGE 3 {}", retMap);

		return retMap;
	}


	public Map<String, Map<String, Double>> getTotalVoucherREMAININGValue(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second, String searchStr) {

		String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;

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
		
		if (retMap.size() > 0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
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
		int cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("-->>{}", cnt);


//		printDocs(collectionName, pipeline);


		
		//now getminimum
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeline.add(BasicDBObject.parse("{$group:{_id:{"+groupByPropName+": '$"+groupByPropName+"',voucherid: '$vouch.voucherId'}, rem:{'$min':'$vouch.remaining'}}}"));

//		common.printDocs(collectionName, pipeline, mongoTemplate);
		cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("-->>{}", cnt);

		
		Bson secondProjectVLBson = BasicDBObject.parse("{$group:{_id:'$_id."+groupByPropName+"', "+
		Constants.VOUCHER_REMAINING+":{'$sum':'$rem'}}}");
		pipeline.add(secondProjectVLBson);

		common.printDocs(collectionName, pipeline, mongoTemplate);
		cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("1 -> {}", cnt);

		List<String> kpisV = new ArrayList<String>();
		kpisV.add(Constants.VOUCHER_REMAINING);
		String sortByField = Constants.VOUCHER_REMAINING;
		common.getResults(pipeline, dir, pgNum, retMap, kpisV, collectionName, sortByField, mongoTemplate);

		if (!second) {
			getTotalOrdersValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalInvoiceValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalOrdersIssued(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalLeakageValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActiveItems(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalActivePAs(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);

			getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, retMap, true, searchStr);


		}

		logger.debug("FINAL    FINAL   LEAKAGE 3 {}", retMap);

		return retMap;
	}

	
	public int getTotalVoucherREMAINING_COUNT(String groupByPropName,
			Map<String, List<String>> argfilter, List<String> yyyymm, String searchStr) {

		String collectionName = Constants.VOUCHER_DETAILS_COLLECTION_NAME;

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
		int cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("-->>{}", cnt);


//		printDocs(collectionName, pipeline);


		
		//now getminimum
//		pipeLine.add(Aggregates.unwind("$vouch", new UnwindOptions().preserveNullAndEmptyArrays(true)));
		pipeline.add(BasicDBObject.parse("{$group:{_id:{"+groupByPropName+": '$"+groupByPropName+"',voucherid: '$vouch.voucherId'}, rem:{'$min':'$vouch.remaining'}}}"));

//		common.printDocs(collectionName, pipeline, mongoTemplate);
		cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("-->>{}", cnt);

		
		Bson secondProjectVLBson = BasicDBObject.parse("{$group:{_id:'$_id."+groupByPropName+"', "+
		Constants.VOUCHER_REMAINING+":{'$sum':'$rem'}}}");
		pipeline.add(secondProjectVLBson);

		common.printDocs(collectionName, pipeline, mongoTemplate);
		cnt = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("1 -> {}", cnt);
		
		return cnt;

	}
	
	
	
	public int getTotalVoucherItemsCOUNT(String groupByPropName, Map<String, List<String>> argfilter, 
			List<String> yyyymm, String searchStr) {

		String collectionName = Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME;

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

		
		Bson expandVouch = BasicDBObject
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
			
			List<Bson> andList = new ArrayList<>();
			for(String ym : yyyymm) {
				LocalDate firstDay = LocalDate.parse(ym+"-01", DateTimeFormatter.ISO_DATE);
				String yymm = firstDay.format(DateTimeFormatter.ofPattern("YYYY-MM"));
				LocalDate lastDay = YearMonth.from(firstDay).atEndOfMonth();
				andList.add(Filters.and(Filters.gte("vouchenddt", firstDay), Filters.lte("vouchstdt", lastDay)));
			}
			
			if(andList.size()>0) {
				pipeline.add(match(and(andList)));
			}
			
		}



//		printDocs(collectionName, pipeline);


		String grpDist = "{$group:{"
				+ "_id:{dim : '$"+groupByPropName+"', vid: '$vouchid'}, "
				+"val :{'$max':'$vouchval'} "
				+ "}}";
		Bson groupdistinct = BasicDBObject.parse(grpDist);
		pipeline.add(groupdistinct);
//		common.printDocs(collectionName, pipeline, mongoTemplate);


		int count = 0;
		count = common.getCount(collectionName, pipeline, mongoTemplate);
		logger.debug("The count VT = {}", count);
		return count;
		
	}

	
	
}
