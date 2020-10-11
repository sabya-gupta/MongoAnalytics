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
public class VoucherTests {

	@Autowired
	VoucherAnalyticsAllLevels voucherAnalyticsAllLevels;

	Logger logger = LoggerFactory.getLogger(getClass());
	
	
	
	@Test
	public void topLevelTestsForVR() {
//		voucherAnalyticsAllLevels.getTotalVoucherKPIs(null, null);

		List<String> yyyymm = new ArrayList<String>();
		yyyymm.add("2020-08");
		voucherAnalyticsAllLevels.getTotalVoucherKPIs(null, yyyymm);
	}	
	@Test
	public void topLevelTests() {
		voucherAnalyticsAllLevels.getTotalVoucherKPIs(null, null);
		
		String grpByPropName = Constants.PROP_SUPPLIER_PART_NUMBER;

		voucherAnalyticsAllLevels.getTotalVoucherKPIs(null, null);
		logger.debug("1 $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

		
		Map<String, List<String>> filter1 = new HashMap<>();

		List<String> l1 = new ArrayList<String>();
		l1.add("E102");
		l1.add("K209");
		filter1.put(Constants.PROP_MATERIAL_GROUP_4, l1);

		List<String> lstSearch = new ArrayList<String>();
		lstSearch.add("CD58C6FB146D4EA691B819295CAD2D95");
		filter1.put(grpByPropName, lstSearch);
		
		voucherAnalyticsAllLevels.getTotalVoucherKPIs(filter1, null);
		logger.debug("2 $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		
		List<String> yyyymm = new ArrayList<String>();
		yyyymm.add("2022-07");
		yyyymm.add("2018-09");

		voucherAnalyticsAllLevels.getTotalVoucherKPIs(null, yyyymm);
		logger.debug("1 END EOF TOP LEVEL TESTS");
	}
	
	
	@Test
	public void getForLast7Days() {
		voucherAnalyticsAllLevels.getTotalVoucherKPIsForLast7Days(null);
		logger.debug("1 END EOF 7 DAYS");
	}
	
	
	@Autowired
	KPIFilterAndGroupByHandler kPIFilterAndGroupByHandler;
	
	@Test
	public void secondLevelTests2() {
		
		
		String groupByPropName = Constants.PROP_MATERIAL_GROUP_4;
		int dir = Constants.SORT_DIRECTION_ASCENDING;
		Map<String, List<String>> argfilter = null;
		int pgNum = 0;
		List<String> yyyymm = null;
		String searchStr = null;
		String orderByKPI = Constants.KPI_VV_VALUE;

		kPIFilterAndGroupByHandler.getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, new HashMap<String, Map<String,Double>>(), false, searchStr);

	}	
		
	@Test
	public void secondLevelTests() {
		
		
		String groupByPropName = Constants.PROP_MATERIAL_GROUP_4;
		int dir = Constants.SORT_DIRECTION_ASCENDING;
		Map<String, List<String>> argfilter = null;
		int pgNum = 0;
		List<String> yyyymm = null;
		String searchStr = null;
		String orderByKPI = Constants.KPI_VV_VALUE;

		kPIFilterAndGroupByHandler.getTotalVoucherREMAININGValue(groupByPropName, dir, argfilter, pgNum, yyyymm, new HashMap<String, Map<String,Double>>(), false, searchStr);

		
		
		
		
		kPIFilterAndGroupByHandler.getDataByProp(groupByPropName, orderByKPI, dir, argfilter, pgNum, yyyymm, searchStr);
		kPIFilterAndGroupByHandler.getDataCOUNTByProp(groupByPropName, orderByKPI, dir, argfilter, yyyymm, searchStr);

		logger.debug("1 END ..... --------------------------------------------------------------------------------------------------");

		searchStr = "e";
		
		kPIFilterAndGroupByHandler.getDataByProp(groupByPropName, orderByKPI, dir, argfilter, pgNum, yyyymm, searchStr);
		kPIFilterAndGroupByHandler.getDataCOUNTByProp(groupByPropName, orderByKPI, dir, argfilter, yyyymm, searchStr);

		logger.debug("2 END....--------------------------------------------------------------------------------------------------");
		yyyymm = new ArrayList<String>();
		yyyymm.add("2022-07");
		kPIFilterAndGroupByHandler.getDataByProp(groupByPropName, orderByKPI, dir, argfilter, pgNum, yyyymm, searchStr);
		
		kPIFilterAndGroupByHandler.getDataCOUNTByProp(groupByPropName, orderByKPI, dir, argfilter, yyyymm, searchStr);

//		kPIFilterAndGroupByHandlerForVoucher.getTotalVoucherValue(Constants.PROP_MATERIAL_GROUP_4, 
//				Constants.SORT_DIRECTION_ASCENDING, null, 0, null, null, false, "42", true);
		logger.debug("1 END EOF SECOND LEVEL");
	}
}
