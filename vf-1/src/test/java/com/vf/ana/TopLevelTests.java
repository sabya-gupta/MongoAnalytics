package com.vf.ana;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class TopLevelTests {

	@Autowired
	PASDAnaRepository pASDAnaRepository;

	@Autowired
	DatePASDAnaRepository datePASDAnaRepository;

	@Autowired
	TopLevelAnalyticsUtilPAandActiveItems topLevelAnalyticsUtil;
	
	@Autowired
	TopLevelAnalyticsUtilOVandIVandnOV topLevelAnalyticsUtilOVandIVandnOV;

	Logger logger = LoggerFactory.getLogger(getClass());
	
	
//	@Test
//	public void getALL_ACTIVE_PAs() {
//		int num = topLevelAnalyticsUtil.getAllActivePAsOrActiveItems(true);
//		logger.debug("ACTIVE OPEN **PAs ALLLLLLL {}", num);
//	}
//	
//	@Test
//	public void getActivePAsByDateWithoutFilter() {
//		String dt = "2020-09-01";
//		int num = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByDate(dt, null, true);
//		logger.debug("ACTIVE OPEN **PAs AS ON {} - {}", dt, num);
//	}
//	
//
//	@Test
//	public void getActivePAsByFilterForADate() {
//		String dt = "2020-09-01";
//		
//		Map<String, List<String>> filter = new HashMap<>();
//		
//		List<String> mgcList = new ArrayList<>();
//		mgcList.add("A212");
//		filter.put("materialGroupL4", mgcList);
//
//		List<String> suppIdLst = new ArrayList<>();
//		suppIdLst.add("11796860");
//		filter.put("parentSupplierId", suppIdLst);
//
//		
//		int num = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByDate(dt, filter, true);
//		logger.debug("ACTIVE OPEN WITH FILTER **PAs AS ON {} - {}", dt, num);
//	}
//
//	@Test
//	public void getActivePAsForLast7Days() {
//		
//		Map<String, List<String>> filter = new HashMap<>();
//		
//		List<String> mgcList = new ArrayList<>();
//		mgcList.add("A213");
//		mgcList.add("A212");
//		filter.put("materialGroupL4", mgcList);
//
//		List<String> suppIdLst = new ArrayList<>();
//		suppIdLst.add("11796860");
//		filter.put("parentSupplierId", suppIdLst);
//
//		//without filter
//		Map<String, Integer> ret = topLevelAnalyticsUtil.getActivePAsOrActiveItemsWithFilterForLast7Days(null, true);
//		logger.debug("ACTIVE OPEN WITHOUT FILTER L7D {}" , ret);
//
//		//with filter
//		ret = topLevelAnalyticsUtil.getActivePAsOrActiveItemsWithFilterForLast7Days(filter, true);
//		logger.debug("ACTIVE OPEN WITH FILTER L7D {}" , ret);
//	}
//
//	@Test
//	public void getActivePAsForDateRange() {
//		
//		Map<String, List<String>> filter = new HashMap<>();
//		
//		List<String> mgcList = new ArrayList<>();
//		mgcList.add("A213");
//		mgcList.add("A212");
//		filter.put("materialGroupL4", mgcList);
//
//		List<String> suppIdLst = new ArrayList<>();
//		suppIdLst.add("11796860");
//		filter.put("parentSupplierId", suppIdLst);
//
//		//without filter
//		Map<String, Integer> ret = topLevelAnalyticsUtil.getActivePAsOrActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-26", null, true);
//		logger.debug("ACTIVE OPEN DATE RANGE WITHOUT FILTER {}" , ret);
//
//		//with filter
//		ret = topLevelAnalyticsUtil.getActivePAsOrActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-26", filter, true);
//		logger.debug("ACTIVE OPEN DATE RANGE  WITH FILTER {}" , ret);
//	}
//
//
//	
//	
//	
//	@Test
//	public void getALL_ACTIVE_ITEMSs() {
//		int num = topLevelAnalyticsUtil.getAllActivePAsOrActiveItems(false);
//		logger.debug("ACTIVE OPEN **ITEMS ALLLLLLL {}", num);
//	}
//	
//	@Test
//	public void getActiveItemsByDateWithoutFilter() {
//		String dt = "2020-09-01";
//		int num = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByDate(dt, null, false);
//		logger.debug("ACTIVE OPEN **ITEMS AS ON {} - {}", dt, num);
//	}
//	
//
//	@Test
//	public void getActiveItemsByFilterForADate() {
//		String dt = "2020-09-01";
//		
//		Map<String, List<String>> filter = new HashMap<>();
//		
//		List<String> mgcList = new ArrayList<>();
//		mgcList.add("A212");
//		filter.put("materialGroupL4", mgcList);
//
//		List<String> suppIdLst = new ArrayList<>();
//		suppIdLst.add("11796860");
//		filter.put("parentSupplierId", suppIdLst);
//
//		
//		int num = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByDate(dt, filter, false);
//		logger.debug("ACTIVE OPEN WITH FILTER **ITEMS AS ON {} - {}", dt, num);
//	}
//
//	@Test
//	public void getActiveItemsForLast7Days() {
//		
//		Map<String, List<String>> filter = new HashMap<>();
//		
//		List<String> mgcList = new ArrayList<>();
//		mgcList.add("A213");
//		mgcList.add("A212");
//		filter.put("materialGroupL4", mgcList);
//
//		List<String> suppIdLst = new ArrayList<>();
//		suppIdLst.add("11796860");
//		filter.put("parentSupplierId", suppIdLst);
//
//		//without filter
//		Map<String, Integer> ret = topLevelAnalyticsUtil.getActivePAsOrActiveItemsWithFilterForLast7Days(null, false);
//		logger.debug("ITEMS OPEN WITHOUT FILTER L7D {}" , ret);
//
//		//with filter
//		ret = topLevelAnalyticsUtil.getActivePAsOrActiveItemsWithFilterForLast7Days(filter, false);
//		logger.debug("ITEMS OPEN WITH FILTER L7D {}" , ret);
//	}
//
//	@Test
//	public void getActiveItemsForDateRange() {
//		
//		Map<String, List<String>> filter = new HashMap<>();
//		
//		List<String> mgcList = new ArrayList<>();
//		mgcList.add("A213");
//		mgcList.add("A212");
//		filter.put("materialGroupL4", mgcList);
//
//		List<String> suppIdLst = new ArrayList<>();
//		suppIdLst.add("11796860");
//		filter.put("parentSupplierId", suppIdLst);
//
//		//without filter
//		Map<String, Integer> ret = topLevelAnalyticsUtil.getActivePAsOrActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-26", null, false);
//		logger.debug("ITEMS OPEN DATE RANGE WITHOUT FILTER {}" , ret);
//
//		//with filter
//		ret = topLevelAnalyticsUtil.getActivePAsOrActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-26", filter, false);
//		logger.debug("ITEMS OPEN DATE RANGE  WITH FILTER {}" , ret);
//	}

	@Test
	public void runPOTests() {
		logger.debug("--------------------------START IVs---------------------------");
		getIVsForVariousCombinationsOfFilterAndDates();
		logger.debug("--------------------------END   IVs---------------------------\n\n");

		logger.debug("--------------------------START OVs---------------------------");
		getOVsForVariousCombinationsOfFilterAndDates();
		logger.debug("--------------------------END   OVs---------------------------\n\n");
		
		logger.debug("--------------------------STARTnPOs---------------------------");
		getnPOsForVariousCombinationsOfFilterAndDates();
		logger.debug("--------------------------END  nPOs---------------------------\n\n");
		
		logger.debug("--------------------------START AIs---------------------------");
		getActiveItemsForVariousCombinationsOfFilterAndDates();
		logger.debug("--------------------------END   AIs---------------------------\n\n");
		
		logger.debug("--------------------------START PAs---------------------------");
		getActivePAsForVariousCombinationsOfFilterAndDates();
		logger.debug("--------------------------END   PAs---------------------------\n\n");
		
		logger.debug("--------------------------STARTSPNs---------------------------");
		aggregateMonthWiseForLastOneYearForSPN();
		logger.debug("--------------------------END  SPNs---------------------------");
		
	}

	public void getActivePAsForVariousCombinationsOfFilterAndDates() {
		
		Map<String, List<String>> filter = new HashMap<>();
		
		List<String> mgcList = new ArrayList<>();
		mgcList.add("A213");
		mgcList.add("A212");
		filter.put("materialGroupL4", mgcList);

		List<String> suppIdLst = new ArrayList<>();
		suppIdLst.add("11796860");
		filter.put("parentSupplierId", suppIdLst);
		
		List<String> yyyymm = new ArrayList<String>();
		yyyymm.add("2020-09");
		yyyymm.add("2018-09");

		//without filter
		int ret = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByFilterAndAlsoDateFilter(null, yyyymm, true);
		logger.debug("ACTIVE PAs OPEN YYYY-MM WITHOUT FILTER {}" , ret);

		//with filter but without date
		ret = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByFilterAndAlsoDateFilter(filter, null, true);
		logger.debug("ACTIVE PAs OPEN YYYY-MM  WITH FILTER {}" , ret);

		//with filter AND with date
		ret = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByFilterAndAlsoDateFilter(filter, yyyymm, true);
		logger.debug("ACTIVE PAs OPEN YYYY-MM  WITH FILTER AND DATE {}" , ret);

		//without filter AND without date (same as all without any filter)
		ret = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByFilterAndAlsoDateFilter(null, null, true);
		logger.debug("ACTIVE PAs OPEN YYYY-MM  WITH FILTER AND DATE {}" , ret);
		
		//Last 7 days
		//without filter
		Map<String, Integer> retMap = topLevelAnalyticsUtil.getActivePAsOrActiveItemsWithFilterForLast7Days(null, true);
		logger.debug("ACTIVE PAs OPEN WITHOUT FILTER L7D {}" , retMap);

		//with filter
		retMap = topLevelAnalyticsUtil.getActivePAsOrActiveItemsWithFilterForLast7Days(filter, true);
		logger.debug("ACTIVE PAs OPEN WITH FILTER L7D {}" , retMap);

		//DATE RANGE
		//without filter
		retMap = topLevelAnalyticsUtil.getActivePAsOrActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-26", null, true);
		logger.debug("ACTIVE PAs OPEN DATE RANGE WITHOUT FILTER {}" , retMap);

		//with filter
		retMap = topLevelAnalyticsUtil.getActivePAsOrActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-26", filter, true);
		logger.debug("ACTIVE PAs OPEN DATE RANGE  WITH FILTER {}" , retMap);
		
		
		//FOR A SINGLE DATE
		String dt = "2020-09-01";

		//with filter
		int num = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByDate(dt, filter, true);
		logger.debug("ACTIVE PAs OPEN WITH FILTER **ITEMS AS ON {} - {}", dt, num);

		//without filter
		num = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByDate(dt, null, true);
		logger.debug("ACTIVE PAs OPEN WITH FILTER **ITEMS AS ON {} - {}", dt, num);
		
		num = topLevelAnalyticsUtil.getAllActivePAsOrActiveItems(true);
		logger.debug("ACTIVE PAs ALLLLLLL {}", num);

	}


	public void getActiveItemsForVariousCombinationsOfFilterAndDates() {
		
		Map<String, List<String>> filter = new HashMap<>();
		
		List<String> mgcList = new ArrayList<>();
		mgcList.add("A213");
		mgcList.add("A212");
		filter.put("materialGroupL4", mgcList);

		List<String> suppIdLst = new ArrayList<>();
		suppIdLst.add("11796860");
		filter.put("parentSupplierId", suppIdLst);
		
		List<String> yyyymm = new ArrayList<String>();
		yyyymm.add("2020-09");
		yyyymm.add("2018-09");

		//without filter
		int ret = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByFilterAndAlsoDateFilter(null, yyyymm, false);
		logger.debug("ITEMS OPEN YYYY-MM WITHOUT FILTER {}" , ret);

		//with filter but without date
		ret = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByFilterAndAlsoDateFilter(filter, null, false);
		logger.debug("ITEMS OPEN YYYY-MM  WITH FILTER {}" , ret);

		//with filter AND with date
		ret = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByFilterAndAlsoDateFilter(filter, yyyymm, false);
		logger.debug("ITEMS OPEN YYYY-MM  WITH FILTER AND DATE {}" , ret);

		//without filter AND without date (same as all without any filter)
		ret = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByFilterAndAlsoDateFilter(null, null, false);
		logger.debug("ITEMS OPEN YYYY-MM  WITH FILTER AND DATE {}" , ret);
		
		//Last 7 days
		//without filter
		Map<String, Integer> retMap = topLevelAnalyticsUtil.getActivePAsOrActiveItemsWithFilterForLast7Days(null, false);
		logger.debug("ITEMS OPEN WITHOUT FILTER L7D {}" , retMap);

		//with filter
		retMap = topLevelAnalyticsUtil.getActivePAsOrActiveItemsWithFilterForLast7Days(filter, false);
		logger.debug("ITEMS OPEN WITH FILTER L7D {}" , retMap);

		//DATE RANGE
		//without filter
		retMap = topLevelAnalyticsUtil.getActivePAsOrActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-26", null, false);
		logger.debug("ITEMS OPEN DATE RANGE WITHOUT FILTER {}" , retMap);

		//with filter
		retMap = topLevelAnalyticsUtil.getActivePAsOrActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-26", filter, false);
		logger.debug("ITEMS OPEN DATE RANGE  WITH FILTER {}" , retMap);

		//FOR A SINGLE DATE
		String dt = "2020-09-01";

		//with filter
		int num = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByDate(dt, filter, false);
		logger.debug("ITEMS OPEN WITH FILTER **ITEMS AS ON {} - {}", dt, num);

		//without filter
		num = topLevelAnalyticsUtil.getTotalActivePAsORActiveItemsByDate(dt, null, false);
		logger.debug("ITEMS OPEN WITH FILTER **ITEMS AS ON {} - {}", dt, num);

		num = topLevelAnalyticsUtil.getAllActivePAsOrActiveItems(false);
		logger.debug("ACTIVE ITEMs ALLLLLLL {}", num);
	}

	
	public void getOVsForVariousCombinationsOfFilterAndDates()
	{
		
		Map<String, List<String>> filter = new HashMap<>();
		
		List<String> mgcList = new ArrayList<>();
		mgcList.add("A213");
		mgcList.add("A212");
		filter.put("materialGroupL4", mgcList);

		List<String> suppIdLst = new ArrayList<>();
		suppIdLst.add("11796860");
		filter.put("parentSupplierId", suppIdLst);
		
		List<String> yyyymm = new ArrayList<String>();
		yyyymm.add("2020-09");
		yyyymm.add("2018-09");

		double num = -1000;

		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(filter, yyyymm, Constants.ORDER_VALUE);
		logger.debug("OVs ALLLLLLL with filter and dates {}", num);

		//without filter
		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(null, yyyymm, Constants.ORDER_VALUE);
		logger.debug("OVs YYYY-MM WITHOUT FILTER {}" , num);

		//with filter but without date
		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(filter, null, Constants.ORDER_VALUE);
		logger.debug("OVs OPEN WITHOUT YYYY-MM  WITH FILTER {}" , num);

		//without filter AND without date (same as all without any filter)
		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(null, null, Constants.ORDER_VALUE);
		logger.debug("OVs WITHOUT FILTER OR YYYY-MM {}" , num);
		
		Map<String, Double> retMap = null;
//		
//		//Last 7 days
//		//without filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersForLast7Days
		(null, Constants.ORDER_VALUE);
		logger.debug("ORDER VAL DATE 7777 FILTER {}" , retMap);
		
		//with filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersForLast7Days(filter, Constants.ORDER_VALUE);
		logger.debug("ORDER VALUE FILTER L777777D {}" , retMap);
//
//		//DATE RANGE
//		//without filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersAndDateRangeByDate
		("2020-09-01", "2020-09-26", null, Constants.ORDER_VALUE);
		logger.debug("ORDER VAL DATE RANGE WITHOUT FILTER {}" , retMap);
//
//		//with filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersAndDateRangeByDate
		("2020-09-01", "2020-09-26", filter, Constants.ORDER_VALUE);
		logger.debug("ORDER VAL DATE RANGE ANDDDD FILTER {}" , retMap);

	}

	
	public void getIVsForVariousCombinationsOfFilterAndDates()
	{
		
		Map<String, List<String>> filter = new HashMap<>();
		
		List<String> mgcList = new ArrayList<>();
		mgcList.add("A213");
		mgcList.add("A212");
		filter.put("materialGroupL4", mgcList);

		List<String> suppIdLst = new ArrayList<>();
		suppIdLst.add("11796860");
		filter.put("parentSupplierId", suppIdLst);
		
		List<String> yyyymm = new ArrayList<String>();
		yyyymm.add("2020-09");
		yyyymm.add("2018-09");

		double num = -1000;

		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(filter, yyyymm, Constants.INVOICE_VALUE);
		logger.debug("IVs ALLLLLLL with filter and dates {}", num);

		//without filter
		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(null, yyyymm, Constants.INVOICE_VALUE);
		logger.debug("IVs YYYY-MM WITHOUT FILTER {}" , num);

		//with filter but without date
		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(filter, null, Constants.INVOICE_VALUE);
		logger.debug("IVs OPEN WITHOUT YYYY-MM  WITH FILTER {}" , num);

		//without filter AND without date (same as all without any filter)
		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(null, null, Constants.INVOICE_VALUE);
		logger.debug("IVs WITHOUT FILTER OR YYYY-MM {}" , num);
//		
		Map<String, Double> retMap = null;
//		//Last 7 days
//		//without filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersForLast7Days
		(null, Constants.INVOICE_VALUE);
		logger.debug("INVOICE_VALUE DATE 7777 FILTER {}" , retMap);
		
		//with filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersForLast7Days(filter, Constants.INVOICE_VALUE);
		logger.debug("INVOICE_VALUE FILTER L777777D {}" , retMap);
//
//		//DATE RANGE
//		//without filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersAndDateRangeByDate
		("2020-09-01", "2020-09-26", null, Constants.INVOICE_VALUE);
		logger.debug("INVOICE VAL DATE RANGE WITHOUT FILTER {}" , retMap);
//
//		//with filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersAndDateRangeByDate
		("2020-09-01", "2020-09-26", filter, Constants.INVOICE_VALUE);
		logger.debug("INVOICE VAL DATE RANGE ANDDDD FILTER {}" , retMap);

	}
	
	public void getnPOsForVariousCombinationsOfFilterAndDates()
	{
		
		Map<String, List<String>> filter = new HashMap<>();
		
		List<String> mgcList = new ArrayList<>();
		mgcList.add("A213");
		mgcList.add("A212");
		filter.put("materialGroupL4", mgcList);

		List<String> suppIdLst = new ArrayList<>();
		suppIdLst.add("11796860");
		filter.put("parentSupplierId", suppIdLst);
		
		List<String> yyyymm = new ArrayList<String>();
		yyyymm.add("2020-09");
		yyyymm.add("2018-09");

		double num = -1000;

		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(filter, yyyymm, Constants.NUMBER_OF_ORDERS);
		logger.debug("NOs ALLLLLLL with filter and dates {}", num);

		//without filter
		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(null, yyyymm, Constants.NUMBER_OF_ORDERS);
		logger.debug("NOs YYYY-MM WITHOUT FILTER {}" , num);

		//with filter but without date
		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(filter, null, Constants.NUMBER_OF_ORDERS);
		logger.debug("NOs OPEN WITHOUT YYYY-MM  WITH FILTER {}" , num);

		//without filter AND without date (same as all without any filter)
		num = topLevelAnalyticsUtilOVandIVandnOV.getAllTotalValuesFromOrdersByFilterAndDates(null, null, Constants.NUMBER_OF_ORDERS);
		logger.debug("NOs WITHOUT FILTER OR YYYY-MM {}" , num);


		Map<String, Double> retMap = null;
		
		//		//Last 7 days
//		//Last 7 days
//		//without filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersForLast7Days
		(null, Constants.NUMBER_OF_ORDERS);
		logger.debug("NUMBER_OF_ORDERS DATE 7777 FILTER {}" , retMap);
		
		//with filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersForLast7Days(filter, Constants.NUMBER_OF_ORDERS);
		logger.debug("NUMBER_OF_ORDERS FILTER L777777D {}" , retMap);
//
		//DATE RANGE
		//without filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersAndDateRangeByDate("2020-09-01", "2020-09-26", null, Constants.NUMBER_OF_ORDERS);
		logger.debug("NUMBER OF ORDERS DATE RANGE WITHOUT FILTER {}" , retMap);

//		//with filter
		retMap = topLevelAnalyticsUtilOVandIVandnOV.getAllValuesFromOrdersByFiltersAndDateRangeByDate("2020-09-01", "2020-09-26", filter, Constants.NUMBER_OF_ORDERS);
		logger.debug("NUMBER OF ORDERS DATE RANGE ANDDD FILTER {}" , retMap);
//
//		//FOR A SINGLE DATE
//		String dt = "2020-09-01";
		
		

//		//with filter
//		num = topLevelAnalyticsUtil.getActivePAsORActiveItemsByDate(dt, filter, false);
//		logger.debug("ITEMS OPEN WITH FILTER **ITEMS AS ON {} - {}", dt, num);

		//without filter
//		num = topLevelAnalyticsUtil.getActivePAsORActiveItemsByDate(dt, null, false);
//		logger.debug("ITEMS OPEN WITH FILTER **ITEMS AS ON {} - {}", dt, num);

	}
	
	@Autowired
	KPIFilterAndGroupByHandler kPIFilterAndGroupByHandler;
	

	public void aggregateMonthWiseForLastOneYearForSPN() {
		String grpByPropName = Constants.PROP_SUPPLIER_PART_NUMBER;

		Map<String, List<String>> filter1 = new HashMap<>();

		List<String> l1 = new ArrayList<String>();
		l1.add("K208");
		l1.add("K209");
		filter1.put(Constants.PROP_MATERIAL_GROUP_4, l1);

		List<String> lstSearch = new ArrayList<String>();
		lstSearch.add("400124668");
		filter1.put(grpByPropName, lstSearch);
		
		
		logger.debug("--------------------------------------");
		kPIFilterAndGroupByHandler.aggregateMonthWiseForLastOneYear(Constants.PROP_SUPPLIER_PART_NUMBER, "A51685615F584DD1A5B65349BAC597D5", filter1);
		kPIFilterAndGroupByHandler.aggregateMonthWiseForLastOneYear(Constants.PROP_SUPPLIER_PART_NUMBER, "A51685615F584DD1A5B65349BAC597D5", null);
//		logger.debug("--------------------------------------");
//		monthWiseAggregation.aggregateMonthWiseForLastOneYear(Constants.PROP_PARENT_SUPPLIER_ID, "11796860", null);
		//testSortAndPaginate.aggregateSPNMonthWise("9A0A04A3603F4F95AB08EA6EFDC6836B");
	}

	
}
