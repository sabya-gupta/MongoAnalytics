package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;

import java.time.LocalDate;
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
import com.mongodb.client.MongoDatabase;

@Repository
public class CombinedDataUtil {

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
			+ "priceUnitPo:{ $ifNull: ['$priceUnitPo', 1 ] },"
			+ "netPricePOPrice:1," 
			+ "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 0 ] }," 
			+ "invoiceQuantity: { $ifNull: ['$invoiceQuantity', 0 ] }," 
			+ "invoiceDate:1,"
			+ "purchaseOrderCreationDate:1," 
			+ "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 1 ] }," + dateStr + "}}";

	
	public Map<String, Map<String, Double>> getTotalOrdersValue(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second) {
		
		
		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}

		if(retMap.size()==0 && second) return retMap;

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

		

		setCommonFiltersForPO(pipeline);


		
		
		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);
		
		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		
		
		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0 && retMap.size()==0) {
			Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		//add already selected filter
		if(retMap.size()>0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

		
		String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "po:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}, "
				+ "}}";
		Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

		
		String projectQIV = "{$group:{_id: '$dim' , " + Constants.ORDER_VALUE + ":{$sum:'$po'} "
				+  "}}";

		Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		
		List<String> kpisV = new ArrayList<String>();
		kpisV.add(Constants.ORDER_VALUE);
		String sortByField = Constants.ORDER_VALUE;
		getResults(pipeline, dir, pgNum, retMap, kpisV, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
				sortByField);
		logger.debug("FINAL    FINAL   OV 3 {}", retMap);

		return retMap;
	}
	

	public Map<String, Map<String, Double>> getTotalInvoiceValue(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second) {
		
		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}
		if(retMap.size()==0 && second) return retMap;

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

		

		setCommonFiltersForPO(pipeline);//tackle wayward dates


		
		
		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0 && retMap.size()==0) {
			Bson dateFilter = match(in("invoiceDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		//add already selected filter
		if(retMap.size()>0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

		
		String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
				+ "iv:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']},"
				+ "}}";
		Bson bPrjOIV = BasicDBObject.parse(projectIV);
		pipeline.add(bPrjOIV);

		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		
		String projectQIV = "{$group:{_id: '$dim' , " 
				+ Constants.INVOICE_VALUE + ":{$sum:'$iv'} }}";

		Bson bPrjOV = BasicDBObject.parse(projectQIV);
		pipeline.add(bPrjOV);

		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);
		
		List<String> kpisV = new ArrayList<String>();
		kpisV.add(Constants.INVOICE_VALUE);
		String sortByField = Constants.INVOICE_VALUE;
		getResults(pipeline, dir, pgNum, retMap, kpisV, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
				sortByField);
		logger.debug("FINAL    FINAL   OV 2 {}", retMap);

		return retMap;
	}
	
	
	
	public Map<String, Map<String, Double>> getTotalOrdersIssued(String groupByPropName, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, boolean second) {

		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}
		
		if(retMap.size()==0 && second) return retMap;


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

		

		setCommonFiltersForPO(pipeline);//tackle wayward dates

		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		
		
		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0 && retMap.size()==0) {
			Bson dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
			pipeline.add(dateFilter);
		}

		//add already selected filter
		if(retMap.size()>0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}

		
		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		String projectPOIss = "{$group:{_id:{dim: '$" + groupByPropName + "', po:'$purchaseOrderNumberOne' }, "
				+ Constants.NUMBER_OF_ORDERS + ":{$sum:1} }}";
		Bson bPrjOIss = BasicDBObject.parse(projectPOIss);
		pipeline.add(bPrjOIss);

		String projectQIss = "{$group:{_id: '$_id.dim' , " + Constants.NUMBER_OF_ORDERS + ":{$sum:1}}} ";

		System.out.println(projectQIss);
		Bson bPrjPOIss = BasicDBObject.parse(projectQIss);
		pipeline.add(bPrjPOIss);

		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		List<String> kpisIss = new ArrayList<String>();
		kpisIss.add(Constants.NUMBER_OF_ORDERS);
		getResults(pipeline, dir, pgNum, retMap, kpisIss, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
				Constants.NUMBER_OF_ORDERS);
		logger.debug("FINAL    FINAL   OISS 3 {}", retMap);
		return retMap;
	}

	
	
	private void printDocs(String collection, List<Bson> pipeline) {
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug(">>>>>>>>>>{}", doc);
		});

	}

	public void getAKPIsByProperty(final String groupByPropName, final String orderByKPIField, int dir,
			final Map<String, List<String>> argfilter, final int pgNum) {

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

		logger.debug("{} - {}", groupByPropName, orderByKPIField);
		Map<String, Map<String, Double>> retMap = new TreeMap<String, Map<String, Double>>();

		if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)) {
			if (filter == null)
				filter = new HashMap<String, List<String>>();
			List<String> activeClause = filter.get(Constants.PROP_PRICEAGG_STATUS);
			if (activeClause == null)
				activeClause = new ArrayList<>();
			if (!activeClause.contains("Active")) {
				activeClause.add("Active");
			}
			filter.put(Constants.PROP_PRICEAGG_STATUS, activeClause);
		}

		List<Bson> pipeline = new ArrayList<>();

		String dateStr = "invoiceDateyymm : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
				+ "purchaseOrderCreationDateyymm : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }}";
		if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)) {
			dateStr = "validFromDatemm : {$dateToString: { format: '%Y-%m', date: '$validFromDate' }}, "
					+ "validToDateyymm : {$dateToString: { format: '%Y-%m', date: '$validToDate' }}";
			;
		}

		String firstProject = "{$project:{supplierPartNumber:1, " + "tradingModel:1, " + "supplierId: 1, "
				+ "outlineAgreementNumber: 1, " + "catalogueType:1, " + "opcoCode:1, " + "priceAgreementStatus:1, "
				+ "parentSupplierId:1, " + "validFromDate:1, " + "validToDate:1, " + "materialGroupL4:1, "
				+ "priceAgreementReferenceName:1," + "quantityOrderedPurchaseOrder:1," + "priceUnitPo:1,"
				+ "netPricePOPrice:1," + "invoiceUnitPriceAsPerTc:1," + "invoiceQuantity:1," + "invoiceDate:1,"
				+ "purchaseOrderCreationDate:1," + "invoiceUnitPriceAsPerTc:1," + dateStr + "}}";
		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		if (filter != null) {
//			String filterClause = common.formMatchClauseForListFilter(filter, null, null);
//			logger.debug("expr {}", filterClause);
//			Bson bfilterClause = BasicDBObject.parse(filterClause);

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		switch (orderByKPIField) {
		case Constants.KPI_ACTIVEPA_COUNT:

			String projectQ = "{$group:{_id:{dim: '$" + groupByPropName
					+ "', spn:'$supplierPartNumber', oc:'$opcoCode' }, " + Constants.ACTIVE_PRICE_AGREEMENT
					+ ":{$sum:1} }}";
			Bson bPrj = BasicDBObject.parse(projectQ);
			pipeline.add(bPrj);

			String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1}}}";
			Bson bagg = BasicDBObject.parse(aggStr);
			pipeline.add(bagg);

			List<String> kpisAPA = new ArrayList<String>();
			kpisAPA.add(Constants.ACTIVE_PRICE_AGREEMENT);
			getResults(pipeline, dir, pgNum, retMap, kpisAPA, Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME,
					Constants.ACTIVE_PRICE_AGREEMENT);
			logger.debug("FINAL    FINAL   APA {}", retMap);
			break;

		case Constants.KPI_ACTIVEITEMS_COUNT:

			String projectQAc = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber' }, "
					+ Constants.ACTIVE_ITEMS + ":{$sum:1} }}";
			Bson bPrjAC = BasicDBObject.parse(projectQAc);
			pipeline.add(bPrjAC);

			String aggStr2 = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_ITEMS + ":{$sum:1}}}";
			Bson bagg2 = BasicDBObject.parse(aggStr2);
			pipeline.add(bagg2);

			List<String> kpisAI = new ArrayList<String>();
			kpisAI.add(Constants.ACTIVE_ITEMS);
			getResults(pipeline, dir, pgNum, retMap, kpisAI, Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME,
					Constants.ACTIVE_ITEMS);
			logger.debug("FINAL    FINAL   AI {}", retMap);

			break;

		case Constants.KPI_INVOICE_VALUE:
		case Constants.KPI_ORDER_VALUE:

			Bson purgeDates1 = match(lte("invoiceDate", LocalDate.now().plusYears(5)));
			Bson purgeDates2 = match(lte("purchaseOrderCreationDate", LocalDate.now().plusYears(5)));

			pipeline.add(purgeDates1);
			pipeline.add(purgeDates2);

			String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
					+ "po:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}, "
					+ "iv:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']},"
					+ "}}";
			Bson bPrjOIV = BasicDBObject.parse(projectIV);
			pipeline.add(bPrjOIV);

			String projectQIV = "{$group:{_id: '$dim' , " + Constants.ORDER_VALUE + ":{$sum:'$po'}, "
					+ Constants.INVOICE_VALUE + ":{$sum:'$iv'} }}";

			System.out.println(projectQIV);
			Bson bPrjOV = BasicDBObject.parse(projectQIV);
			pipeline.add(bPrjOV);

			List<String> kpisV = new ArrayList<String>();
			kpisV.add(Constants.ORDER_VALUE);
			kpisV.add(Constants.INVOICE_VALUE);
			String sortByField = Constants.INVOICE_VALUE;
			if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ORDER_VALUE)) {
				sortByField = Constants.ORDER_VALUE;
			}
			getResults(pipeline, dir, pgNum, retMap, kpisV, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
					sortByField);
			logger.debug("FINAL    FINAL   OV {}", retMap);
		default:
			break;

		}
	}

	public void getAKPIsByPropertyWithMonthFilter(String groupByPropName, String orderByKPIField, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm) {

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

		logger.debug("{} - {}", groupByPropName, orderByKPIField);
		Map<String, Map<String, Double>> retMap = new TreeMap<String, Map<String, Double>>();

		if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)) {
			if (filter == null)
				filter = new HashMap<String, List<String>>();
			List<String> activeClause = filter.get(Constants.PROP_PRICEAGG_STATUS);
			if (activeClause == null)
				activeClause = new ArrayList<>();
			if (!activeClause.contains("Active")) {
				activeClause.add("Active");
			}
			filter.put(Constants.PROP_PRICEAGG_STATUS, activeClause);
		}

		List<Bson> pipeline = new ArrayList<>();


		if (filter != null) {
//			String filterClause = common.formMatchClauseForListFilter(filter, null, null);
//			logger.debug("expr {}", filterClause);
//			Bson bfilterClause = BasicDBObject.parse(filterClause);

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		// add dateFilters
		if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)) {
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
		}
		String dateStr = "invoiceDateyymm : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
				+ "purchaseOrderCreationDateyymm : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }}";
		if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)) {
			dateStr = "validFromDatemm : {$dateToString: { format: '%Y-%m', date: '$validFromDate' }}, "
					+ "validToDateyymm : {$dateToString: { format: '%Y-%m', date: '$validToDate' }}";
		}

		String firstProject = "{$project:{supplierPartNumber:1, " + "tradingModel:1, " + "supplierId: 1, "
				+ "outlineAgreementNumber: 1, " + "catalogueType:1, " + "opcoCode:1, " + "priceAgreementStatus:1, "
				+ "parentSupplierId:1, " + "validFromDate:1, " + "validToDate:1, " + "materialGroupL4:1, "
				+ "priceAgreementReferenceName:1," + "quantityOrderedPurchaseOrder:1," + "priceUnitPo:1,"
				+ "netPricePOPrice:1," + "invoiceUnitPriceAsPerTc:1," + "invoiceQuantity:1," + "invoiceDate:1,"
				+ "purchaseOrderCreationDate:1," + "invoiceUnitPriceAsPerTc:1," + dateStr + "}}";
		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		switch (orderByKPIField) {
		case Constants.KPI_ACTIVEPA_COUNT:

			String projectQ = "{$group:{_id:{dim: '$" + groupByPropName
					+ "', spn:'$supplierPartNumber', oc:'$opcoCode' }, " + Constants.ACTIVE_PRICE_AGREEMENT
					+ ":{$sum:1} }}";
			Bson bPrj = BasicDBObject.parse(projectQ);
			pipeline.add(bPrj);

			String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1}}}";
			Bson bagg = BasicDBObject.parse(aggStr);
			pipeline.add(bagg);

			List<String> kpisAPA = new ArrayList<String>();
			kpisAPA.add(Constants.ACTIVE_PRICE_AGREEMENT);
			getResults(pipeline, dir, pgNum, retMap, kpisAPA, Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME,
					Constants.ACTIVE_PRICE_AGREEMENT);
			logger.debug("FINAL    FINAL   APA 2 {}", retMap);
			break;

		case Constants.KPI_ACTIVEITEMS_COUNT:

			String projectQAc = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber' }, "
					+ Constants.ACTIVE_ITEMS + ":{$sum:1} }}";
			Bson bPrjAC = BasicDBObject.parse(projectQAc);
			pipeline.add(bPrjAC);

			String aggStr2 = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_ITEMS + ":{$sum:1}}}";
			Bson bagg2 = BasicDBObject.parse(aggStr2);
			pipeline.add(bagg2);

			List<String> kpisAI = new ArrayList<String>();
			kpisAI.add(Constants.ACTIVE_ITEMS);
			getResults(pipeline, dir, pgNum, retMap, kpisAI, Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME,
					Constants.ACTIVE_ITEMS);
			logger.debug("FINAL    FINAL   AI 2 {}", retMap);

			break;

		case Constants.KPI_INVOICE_VALUE:
		case Constants.KPI_ORDER_VALUE:

			Bson purgeDates1 = match(lte("invoiceDate", LocalDate.now().plusYears(5)));
			Bson purgeDates2 = match(lte("purchaseOrderCreationDate", LocalDate.now().plusYears(5)));

			pipeline.add(purgeDates1);
			pipeline.add(purgeDates2);

			// add dateFilters
			if (yyyymm != null && yyyymm.size() > 0) {
				Bson dateFilter = match(in("invoiceDateyymm", yyyymm));
				if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ORDER_VALUE)) {
					dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
				}
				pipeline.add(dateFilter);
			}

			String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
					+ "po:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}, "
					+ "iv:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']},"
					+ "}}";
			Bson bPrjOIV = BasicDBObject.parse(projectIV);
			pipeline.add(bPrjOIV);

			String projectQIV = "{$group:{_id: '$dim' , " + Constants.ORDER_VALUE + ":{$sum:'$po'}, "
					+ Constants.INVOICE_VALUE + ":{$sum:'$iv'} }}";

			System.out.println(projectQIV);
			Bson bPrjOV = BasicDBObject.parse(projectQIV);
			pipeline.add(bPrjOV);

			List<String> kpisV = new ArrayList<String>();
			kpisV.add(Constants.ORDER_VALUE);
			kpisV.add(Constants.INVOICE_VALUE);
			String sortByField = Constants.INVOICE_VALUE;
			if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ORDER_VALUE)) {
				sortByField = Constants.ORDER_VALUE;
			}
			getResults(pipeline, dir, pgNum, retMap, kpisV, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
					sortByField);
		default:
			break;

		}
	}

	public Map<String, Map<String, Double>> getAKPIsByPropertyWithMonthFilterAndCallBack(String groupByPropName, String orderByKPIField, int dir,
			Map<String, List<String>> argfilter, int pgNum, List<String> yyyymm,
			Map<String, Map<String, Double>> retMap, String initialOrderBy) {

		if (retMap == null) {
			retMap = new LinkedHashMap<String, Map<String, Double>>();
		}

		
		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			for (String key : argfilter.keySet()) {
				filter.put(key, argfilter.get(key));
			}
		}

		logger.debug("{} - {}", groupByPropName, orderByKPIField);


		if (
				orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| 
				orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)
				||
				initialOrderBy.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| 
				initialOrderBy.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)
						
			) {
			if (filter == null)
				filter = new HashMap<String, List<String>>();
			List<String> activeClause = filter.get(Constants.PROP_PRICEAGG_STATUS);
			if (activeClause == null)
				activeClause = new ArrayList<>();
			if (!activeClause.contains("Active")) {
				activeClause.add("Active");
			}
			filter.put(Constants.PROP_PRICEAGG_STATUS, activeClause);
		}

		List<Bson> pipeline = new ArrayList<>();

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}

		// add dateFilters
		if (
				orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| 
				orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)
				||
				initialOrderBy.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| 
				initialOrderBy.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)
						
			) {
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
		}
		
		if (
				orderByKPIField.equalsIgnoreCase(Constants.KPI_INVOICE_VALUE)
				|| 
				orderByKPIField.equalsIgnoreCase(Constants.KPI_ORDER_VALUE)
				||
				orderByKPIField.equalsIgnoreCase(Constants.KPI_ORDERS_ISSUED)
				|| 
				initialOrderBy.equalsIgnoreCase(Constants.KPI_INVOICE_VALUE)
				|| 
				initialOrderBy.equalsIgnoreCase(Constants.KPI_ORDER_VALUE)
				||
				initialOrderBy.equalsIgnoreCase(Constants.KPI_ORDERS_ISSUED)
						
			) 
		{
			setCommonFiltersForPO(pipeline);
			
			// add dateFilters
			if (yyyymm != null && yyyymm.size() > 0) {
				Bson dateFilter = match(in("invoiceDateyymm", yyyymm));
				if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ORDER_VALUE)) {
					dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
				}
				pipeline.add(dateFilter);
			}

			
		}


		//add already selected filter
		if(retMap.size()>0) {
			pipeline.add(match(in(groupByPropName, retMap.keySet())));
		}
		
		
		String dateStr = "invoiceDateyymm : {$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, "
				+ "purchaseOrderCreationDateyymm : {$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }}";
		if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEPA_COUNT)
				|| orderByKPIField.equalsIgnoreCase(Constants.KPI_ACTIVEITEMS_COUNT)) {
			dateStr = "validFromDatemm : {$dateToString: { format: '%Y-%m', date: '$validFromDate' }}, "
					+ "validToDateyymm : {$dateToString: { format: '%Y-%m', date: '$validToDate' }}";
		}

		String firstProject = "{$project:{supplierPartNumber:1, " + "tradingModel:1, " + "supplierId: 1, "
				+ "outlineAgreementNumber: 1, " + "catalogueType:1, " + "opcoCode:1, " + "priceAgreementStatus:1, "
				+ "parentSupplierId:1, " + "validFromDate:1, " + "validToDate:1, " + "materialGroupL4:1, "
				+ "priceAgreementReferenceName:1," 
				+ "quantityOrderedPurchaseOrder:{ $ifNull: ['$quantityOrderedPurchaseOrder', 0 ] }," 
				+ "priceUnitPo:{ $ifNull: ['$priceUnitPo', 1 ] },"
				+ "netPricePOPrice:1," 
				+ "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 0 ] }," 
				+ "invoiceQuantity: { $ifNull: ['$invoiceQuantity', 0 ] }," 
				+ "invoiceDate:1,"
				+ "purchaseOrderCreationDate:1," 
				+ "invoiceUnitPriceAsPerTc: { $ifNull: ['$invoiceUnitPriceAsPerTc', 1 ] }," + dateStr + "}}";
		Bson firstProjectBson = BasicDBObject.parse(firstProject);
		pipeline.add(firstProjectBson);

		switch (orderByKPIField) {
		case Constants.KPI_ACTIVEPA_COUNT:

			String projectQ = "{$group:{_id:{dim: '$" + groupByPropName
					+ "', spn:'$supplierPartNumber', oc:'$opcoCode' }, " + Constants.ACTIVE_PRICE_AGREEMENT
					+ ":{$sum:1} }}";
			Bson bPrj = BasicDBObject.parse(projectQ);
			pipeline.add(bPrj);

			String aggStr = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_PRICE_AGREEMENT + ":{$sum:1}}}";
			Bson bagg = BasicDBObject.parse(aggStr);
			pipeline.add(bagg);

			List<String> kpisAPA = new ArrayList<String>();
			kpisAPA.add(Constants.ACTIVE_PRICE_AGREEMENT);
			getResults(pipeline, dir, pgNum, retMap, kpisAPA, Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME,
					Constants.ACTIVE_PRICE_AGREEMENT);
			logger.debug("FINAL    FINAL   APA 2 {}", retMap);
			return retMap;

		case Constants.KPI_ACTIVEITEMS_COUNT:

			String projectQAc = "{$group:{_id:{dim: '$" + groupByPropName + "', spn:'$supplierPartNumber' }, "
					+ Constants.ACTIVE_ITEMS + ":{$sum:1} }}";
			Bson bPrjAC = BasicDBObject.parse(projectQAc);
			pipeline.add(bPrjAC);

			String aggStr2 = "{$group:{_id:'$_id.dim', " + Constants.ACTIVE_ITEMS + ":{$sum:1}}}";
			Bson bagg2 = BasicDBObject.parse(aggStr2);
			pipeline.add(bagg2);

			List<String> kpisAI = new ArrayList<String>();
			kpisAI.add(Constants.ACTIVE_ITEMS);
			getResults(pipeline, dir, pgNum, retMap, kpisAI, Constants.PRICE_AGREEMENT_SPN_DETAILS_COLLECTION_NAME,
					Constants.ACTIVE_ITEMS);
			logger.debug("FINAL    FINAL   AI 2 {}", retMap);
			return retMap;

		case Constants.KPI_INVOICE_VALUE:
		case Constants.KPI_ORDER_VALUE:



			String projectIV = "{$project : {" + "dim: '$" + groupByPropName + "', "
					+ "po:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}, "
					+ "iv:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']},"
					+ "}}";
			Bson bPrjOIV = BasicDBObject.parse(projectIV);
			pipeline.add(bPrjOIV);

			String projectQIV = "{$group:{_id: '$dim' , " + Constants.ORDER_VALUE + ":{$sum:'$po'}, "
					+ Constants.INVOICE_VALUE + ":{$sum:'$iv'} }}";

			System.out.println(projectQIV);
			Bson bPrjOV = BasicDBObject.parse(projectQIV);
			pipeline.add(bPrjOV);

			List<String> kpisV = new ArrayList<String>();
			kpisV.add(Constants.ORDER_VALUE);
			kpisV.add(Constants.INVOICE_VALUE);
			String sortByField = Constants.INVOICE_VALUE;
			if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ORDER_VALUE)) {
				sortByField = Constants.ORDER_VALUE;
			}
			getResults(pipeline, dir, pgNum, retMap, kpisV, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
					sortByField);
			logger.debug("FINAL    FINAL   OV 2 {}", retMap);
			return retMap;
			
		case Constants.KPI_ORDERS_ISSUED:


			// add dateFilters
			if (yyyymm != null && yyyymm.size() > 0) {
				Bson dateFilter = match(in("invoiceDateyymm", yyyymm));
				if (orderByKPIField.equalsIgnoreCase(Constants.KPI_ORDER_VALUE)) {
					dateFilter = match(in("purchaseOrderCreationDateyymm", yyyymm));
				}
				pipeline.add(dateFilter);
			}
			
			String projectPOIss = "{$group:{_id:{dim: '$" + groupByPropName + "', po:'$purchaseOrderNumberOne' }, "
					+ Constants.NUMBER_OF_ORDERS + ":{$sum:1} }}";
			Bson bPrjOIss = BasicDBObject.parse(projectPOIss);
			pipeline.add(bPrjOIss);

			String projectQIss = "{$group:{_id: '$_id.dim' , " + Constants.NUMBER_OF_ORDERS + ":{$sum:1}}} ";

			System.out.println(projectQIss);
			Bson bPrjPOIss = BasicDBObject.parse(projectQIss);
			pipeline.add(bPrjPOIss);

			List<String> kpisIss = new ArrayList<String>();
			kpisIss.add(Constants.NUMBER_OF_ORDERS);
			getResults(pipeline, dir, pgNum, retMap, kpisIss, Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME,
					Constants.INVOICE_VALUE);
			logger.debug("FINAL    FINAL   OISS 3 {}", retMap);
			return retMap;
		default:
			return retMap;
			
		}
	}

	
	private void setCommonFiltersForPO(List<Bson> pipeline) {
		Bson purgeDates1 = match(lte("invoiceDate", LocalDate.now().plusYears(5)));
		Bson purgeDates2 = match(lte("purchaseOrderCreationDate", LocalDate.now().plusYears(5)));

		pipeline.add(purgeDates1);
		pipeline.add(purgeDates2);
	}
	
	private void getResults(List<Bson> pipeline, int dir, int pgNum, final Map<String, Map<String, Double>> retMap,
			List<String> kpis, String collection, String orderByField) {

		if (dir == Constants.SORT_DIRECTION_ASCENDING) {
			String ordStr = "{$sort:{" + orderByField + ":1}}";
			Bson bord = BasicDBObject.parse(ordStr);
			pipeline.add(bord);
		}
		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		if (dir == Constants.SORT_DIRECTION_DESCENDING) {
			String ordStr = "{$sort:{" + orderByField + ":-1}}";
			Bson bord = BasicDBObject.parse(ordStr);
			pipeline.add(bord);
		}
		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

//		if(retMap.size()==0) {
			String skipStr = "{$skip:" + (pgNum * Constants.PAGE_SIZE) + "}}";
			Bson bskp = BasicDBObject.parse(skipStr);
			pipeline.add(bskp);

			String limit = "{$limit:" + Constants.PAGE_SIZE + "}}";
			Bson blim = BasicDBObject.parse(limit);
			pipeline.add(blim);
//		}
		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);


		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug(">>>>>>>>>>{}", doc);
			String key = doc.getString("_id");
			Map<String, Double> tmpMap = retMap.get(key);
			if (tmpMap == null)
				tmpMap = new HashMap<>();
			for (String kpi : kpis) {
				if (kpi.equalsIgnoreCase(Constants.ACTIVE_ITEMS)
						|| kpi.equalsIgnoreCase(Constants.ACTIVE_PRICE_AGREEMENT)
						|| kpi.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS))
					tmpMap.put(kpi, doc.getInteger(kpi).doubleValue());
				else
					tmpMap.put(kpi, doc.getDouble(kpi));
			}
			retMap.put(key, tmpMap);
		});

	}

}
