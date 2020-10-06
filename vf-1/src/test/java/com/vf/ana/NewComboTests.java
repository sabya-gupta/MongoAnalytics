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
public class NewComboTests {

	@Autowired
	CombinedDataUtil combinedDataUtil;
	
	@Autowired
	KPIFilterAndGroupByHandler kPIFilterAndGroupByHandler;
	
	Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	public void test123() {
//		Map<String, List<String>> filter = new HashMap<>();
//		List<String> l1 = new ArrayList<String>();
////		l1.add("'K208'");
////		l1.add("'K209'");
//		l1.add("K208");
//		l1.add("K209");
//		filter.put(Constants.PROP_MATERIAL_GROUP_4, l1);
//		
//		List<String> dates = new ArrayList<String>();
//		dates.add("1800-05");
//		dates.add("2020-05");
		
		
//		combinedDataUtil.getAKPIsByPropertyWithMonthFilter(Constants.PROP_SUPPLIER_PART_NUMBER, Constants.KPI_ACTIVEPA_COUNT, 0, filter, 2, dates);
//		combinedDataUtil.getAKPIsByProperty(Constants.PROP_SUPPLIER_PART_NUMBER, Constants.KPI_ACTIVEPA_COUNT, 0, filter, 2);
//		combinedDataUtil.getAKPIsByProperty(Constants.PROP_SUPPLIER_PART_NUMBER, Constants.KPI_ACTIVEITEMS_COUNT, 0, filter, 2);
//		combinedDataUtil.getAKPIsByProperty(Constants.PROP_PARENT_SUPPLIER_ID, Constants.KPI_ACTIVEITEMS_COUNT, 0, filter, 0);
//		combinedDataUtil.getAKPIsByProperty(Constants.PROP_PARENT_SUPPLIER_ID, Constants.KPI_INVOICE_VALUE, 0, filter, 0);
//		combinedDataUtil.getAKPIsByProperty(Constants.PROP_PARENT_SUPPLIER_ID, Constants.KPI_INVOICE_VALUE, 0, null, 0);
//		combinedDataUtil.getAKPIsByPropertyWithMonthFilter(Constants.PROP_PARENT_SUPPLIER_ID, Constants.KPI_INVOICE_VALUE, 0, null, 0, dates);
//		combinedDataUtil.getAKPIsByPropertyWithMonthFilter(Constants.PROP_PARENT_SUPPLIER_ID, Constants.KPI_ORDER_VALUE, 1, null, 0, dates);
//		combinedDataUtil.getAKPIsByProperty(Constants.PROP_PARENT_SUPPLIER_ID, Constants.KPI_INVOICE_VALUE, 0, null, 1);
//		Map<String, Map<String, Double>> retMap = combinedDataUtil.getAKPIsByPropertyWithMonthFilterAndCallBack(Constants.PROP_PARENT_SUPPLIER_ID, Constants.KPI_INVOICE_VALUE, 0, null, 0, dates, null);
//		logger.debug("1@@@@@@@@@@@@@@@@{}", retMap);
//		retMap = combinedDataUtil.getAKPIsByPropertyWithMonthFilterAndCallBack(Constants.PROP_PARENT_SUPPLIER_ID, Constants.KPI_ACTIVEITEMS_COUNT, 0, null, 0, dates, retMap);
//		logger.debug("2@@@@@@@@@@@@@@@@{}", retMap);
	}
	
	@Test
	public void getAllKPI() {
//		Map<String, List<String>> filter1 = new HashMap<>();
//		List<String> l1 = new ArrayList<String>();
////		l1.add("'K208'");
////		l1.add("'K209'");
//		l1.add("K208");
//		l1.add("K209");
//		filter1.put(Constants.PROP_MATERIAL_GROUP_4, l1);
//		
//		List<String> dates = new ArrayList<String>();
//		dates.add("1800-05");
//		dates.add("2020-05");
//		
//		String grpByPropName = Constants.PROP_PARENT_SUPPLIER_ID;
//		String initialOrderBy = Constants.KPI_INVOICE_VALUE;
//		
//		Map<String, List<String>> filter = null;
//		
////		Map<String, Map<String, Double>> retMap = kPIFilterAndGroupByHandler.getTotalActiveItems(grpByPropName, 0, filter, 0, dates, null, false);
////		logger.debug(">>>:::Size = {}", retMap.size());
//
//		Map<String, Map<String, Double>> ret = kPIFilterAndGroupByHandler.getDataByProp
//				(grpByPropName, Constants.KPI_ORDER_VALUE, 0, filter, 1, dates);
//		logger.debug("{}", ret);
//		
//		int count = kPIFilterAndGroupByHandler.getDataCOUNTByProp
//				(grpByPropName, Constants.KPI_ORDER_VALUE, 0, filter, dates);
//		logger.debug("{}", count);
//		
//		
//		//retMap = kPIFilterAndGroupByHandler.(grpByPropName, 0, filter, 0, dates, retMap, true);
//		//retMap = kPIFilterAndGroupByHandler.getTotalOrdersValue(grpByPropName, 0, filter, 0, dates, retMap, true);
////		Map<String, Map<String, Double>> retMap = combinedDataUtil.getAKPIsByPropertyWithMonthFilterAndCallBack
////				(grpByPropName, initialOrderBy, 0, filter, 1, dates, null, initialOrderBy);
////		logger.debug("1@@@@@@@@@@@@@@@@{}", retMap);
////		
////		retMap = combinedDataUtil.getAKPIsByPropertyWithMonthFilterAndCallBack
////				(grpByPropName, Constants.KPI_ACTIVEITEMS_COUNT, 0, filter, 0, dates, retMap, initialOrderBy);
////		logger.debug("2@@@@@@@@@@@@@@@@{}", retMap);
////
////		retMap = combinedDataUtil.getAKPIsByPropertyWithMonthFilterAndCallBack
////				(grpByPropName, Constants.KPI_ORDERS_ISSUED, 0, filter, 0, dates, retMap, initialOrderBy);
////		logger.debug("3@@@@@@@@@@@@@@@@{}", retMap);
////
////		retMap = combinedDataUtil.getAKPIsByPropertyWithMonthFilterAndCallBack
////				(grpByPropName, Constants.KPI_ACTIVEPA_COUNT, 0, filter, 0, dates, retMap, initialOrderBy);
////		logger.debug("4@@@@@@@@@@@@@@@@{}", retMap);
////
////		retMap = combinedDataUtil.getAKPIsByPropertyWithMonthFilterAndCallBack
////				(grpByPropName, Constants.KPI_ACTIVEITEMS_COUNT, 0, filter, 0, dates, retMap, initialOrderBy);
////		logger.debug("5@@@@@@@@@@@@@@@@{}", retMap);
	}

	
	
	/**
	 * groupby = by which property you want to group - like parentsupplierid, matL4, localmarket etc.
	 * Orderby: by what KPI you want to order by? e.g: OrderValue, Invoice value, ActiveItems etc. Null means no order value.
	 */
	@Test
	public void getAllKPIWithFilter() {

		String grpByPropName = Constants.PROP_SUPPLIER_PART_NUMBER;
//		String kpiToOrderBy = Constants.KPI_ACTIVEPA_COUNT;
//		String kpiToOrderBy = Constants.KPI_ORDER_VALUE;
		String kpiToOrderBy = Constants.KPI_VVREMAINING_VALUE;

		
		Map<String, List<String>> filter1 = new HashMap<>();

		List<String> l1 = new ArrayList<String>();
		l1.add("K208");
		l1.add("K209");
		filter1.put(Constants.PROP_MATERIAL_GROUP_4, l1);

//		List<String> lstSearch = new ArrayList<String>();
//		lstSearch.add("400124668");
//		filter1.put(grpByPropName, lstSearch);
		
		List<String> dates = new ArrayList<String>();
		dates.add("1800-05");
		dates.add("2020-05");
		
		
		String searchStr = null; //"bbb"; //"80";
		
		
		Map<String, List<String>> filter = null;
		
		Map<String, Map<String, Double>> ret = kPIFilterAndGroupByHandler.getDataByProp
				(grpByPropName, kpiToOrderBy, Constants.SORT_DIRECTION_DESCENDING, filter, 0, null, searchStr);
		logger.debug("Final result : {} - nItems = {}", ret, ret.size());
		
//		int count = kPIFilterAndGroupByHandler.getDataCOUNTByProp
//				(grpByPropName, kpiToOrderBy, 0, filter, dates, searchStr);
//		logger.debug("Final result : {}", count);
		
	}
	

}
