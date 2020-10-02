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
public class LeakageTestSecondLevel {

	@Autowired
	KPIFilterAndGroupByHandler kPIFilterAndGroupByHandler;
	
	Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	TopLevelAnalyticsUtilLeakageValue topLevelAnalyticsUtilLeakageValue;
	
	
	@Test
	public void getAllKPIWithFilter() {

		String grpByPropName = Constants.PROP_SUPPLIER_PART_NUMBER;
		String kpiToOrderBy = Constants.KPI_VLIDENTIFIED_COUNT;

		
		Map<String, List<String>> filter1 = new HashMap<>();

		List<String> l1 = new ArrayList<String>();
//		l1.add("K208");
//		l1.add("K209");
//		filter1.put(Constants.PROP_MATERIAL_GROUP_4, l1);

//		List<String> lstSearch = new ArrayList<String>();
//		lstSearch.add("0400002261");
//		filter1.put(grpByPropName, lstSearch);
		
		List<String> lstSearch = new ArrayList<String>();
		lstSearch.add("Buy From");
		filter1.put(Constants.PROP_TRADING_MODEL, lstSearch);
				
		List<String> dates = new ArrayList<String>();
		dates.add("1800-05");
		dates.add("2020-09");
		
		
		String searchStr = null; //"bbb"; //"80";
		
		
		Map<String, List<String>> filter = filter1;
		
		
		//Get data based on property like SPN
		Map<String, Map<String, Double>> ret = kPIFilterAndGroupByHandler.getDataByProp
				(grpByPropName, kpiToOrderBy, Constants.SORT_DIRECTION_DESCENDING, filter, 0, dates, searchStr);
		logger.debug("Final result : {} - nItems = {}", ret, ret.size());
		
		//get count
		int count = kPIFilterAndGroupByHandler.getDataCOUNTByProp
				(grpByPropName, kpiToOrderBy, 0, filter, dates, searchStr);
		logger.debug("Final result : of COUNT {}", count);
		
		
		
		//GET LAST ONE YEAR MONTHWISE
		Map<String, List<String>> filter2 = new HashMap<>();
		List<String> lstSpn = new ArrayList<String>();
		lstSpn.add("305E11FE00894AB0AD172C681ECA1F30");
//		filter2.put(Constants.PROP_SUPPLIER_PART_NUMBER, lstSpn);
		
		
		List<String> lstTM = new ArrayList<String>();
		lstTM.add("Buy From");
		filter2.put(Constants.PROP_TRADING_MODEL, lstTM);
		Map<String, Double> ret2 = kPIFilterAndGroupByHandler.getTotalLeakageValueForLastOneYearMonthWise(null);
		logger.debug("Final result : of LAST 1 YR NULL {}", ret2);
		
		ret2 = kPIFilterAndGroupByHandler.getTotalLeakageValueForLastOneYearMonthWise(filter2);
		logger.debug("Final result : of LAST 1 YR {}", ret2);
		
		
		//GET TOTAL LEAKAGE
		double d = topLevelAnalyticsUtilLeakageValue.getTotalLeakageValue(filter1, dates);
		logger.debug("LEAKAGE COUNT 1 = {}", d);

		d = topLevelAnalyticsUtilLeakageValue.getTotalLeakageValue(null, dates);
		logger.debug("LEAKAGE COUNT 2 = {}", d);

		d = topLevelAnalyticsUtilLeakageValue.getTotalLeakageValue(null, null);
		logger.debug("LEAKAGE COUNT 3 = {}", d);
		
		
	}
}
