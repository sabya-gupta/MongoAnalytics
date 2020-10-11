package com.vf.ana;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class NewVoucherTest {

	Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	KPIFilterAndGroupByHandler kPIFilterAndGroupByHandler;

	@Test
	public void secondLevelTestsForVoucherTotal() {

		final String groupByPropName = Constants.PROP_MATERIAL_GROUP_4;
		final int dir = Constants.SORT_DIRECTION_ASCENDING;
		final Map<String, List<String>> argfilter = null;
		final int pgNum = 0;
		List<String> yyyymm = null;
		String searchStr = null;
		final String orderByKPI = Constants.KPI_VV_VALUE;

//		kPIFilterAndGroupByHandler.getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, new HashMap<>(), false, searchStr);

		kPIFilterAndGroupByHandler.getDataByProp(groupByPropName, orderByKPI, dir, argfilter, pgNum, yyyymm, searchStr);
		kPIFilterAndGroupByHandler.getDataCOUNTByProp(groupByPropName, orderByKPI, dir, argfilter, yyyymm, searchStr);

		logger.debug("1 END ..... --------------------------------------------------------------------------------------------------");

		searchStr = "e";

		kPIFilterAndGroupByHandler.getDataByProp(groupByPropName, orderByKPI, dir, argfilter, pgNum, yyyymm, searchStr);
		kPIFilterAndGroupByHandler.getDataCOUNTByProp(groupByPropName, orderByKPI, dir, argfilter, yyyymm, searchStr);

		logger.debug("2 END....--------------------------------------------------------------------------------------------------");
		yyyymm = new ArrayList<>();
		yyyymm.add("2022-07");
		kPIFilterAndGroupByHandler.getDataByProp(groupByPropName, orderByKPI, dir, argfilter, pgNum, yyyymm, searchStr);

		kPIFilterAndGroupByHandler.getDataCOUNTByProp(groupByPropName, orderByKPI, dir, argfilter, yyyymm, searchStr);

//		kPIFilterAndGroupByHandlerForVoucher.getTotalVoucherValue(Constants.PROP_MATERIAL_GROUP_4, 
//				Constants.SORT_DIRECTION_ASCENDING, null, 0, null, null, false, "42", true);
		logger.debug("1 END EOF SECOND LEVEL");
	}

	@Test
	public void secondLevelTestsForVoucherRemaining() {

		final String groupByPropName = Constants.PROP_MATERIAL_GROUP_4;
		final int dir = Constants.SORT_DIRECTION_ASCENDING;
		final Map<String, List<String>> argfilter = null;
		final int pgNum = 0;
		List<String> yyyymm = null;
		String searchStr = null;
		final String orderByKPI = Constants.KPI_VVREMAINING_VALUE;

//		kPIFilterAndGroupByHandler.getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, new HashMap<>(), false, searchStr);

		kPIFilterAndGroupByHandler.getDataByProp(groupByPropName, orderByKPI, dir, argfilter, pgNum, yyyymm, searchStr);
		kPIFilterAndGroupByHandler.getDataCOUNTByProp(groupByPropName, orderByKPI, dir, argfilter, yyyymm, searchStr);

		logger.debug("1 END ..... --------------------------------------------------------------------------------------------------");

		searchStr = "e";

		kPIFilterAndGroupByHandler.getDataByProp(groupByPropName, orderByKPI, dir, argfilter, pgNum, yyyymm, searchStr);
		kPIFilterAndGroupByHandler.getDataCOUNTByProp(groupByPropName, orderByKPI, dir, argfilter, yyyymm, searchStr);

		logger.debug("2 END....--------------------------------------------------------------------------------------------------");
		yyyymm = new ArrayList<>();
		yyyymm.add("2022-07");
		kPIFilterAndGroupByHandler.getDataByProp(groupByPropName, orderByKPI, dir, argfilter, pgNum, yyyymm, searchStr);

		kPIFilterAndGroupByHandler.getDataCOUNTByProp(groupByPropName, orderByKPI, dir, argfilter, yyyymm, searchStr);

//		kPIFilterAndGroupByHandlerForVoucher.getTotalVoucherValue(Constants.PROP_MATERIAL_GROUP_4, 
//				Constants.SORT_DIRECTION_ASCENDING, null, 0, null, null, false, "42", true);
		logger.debug("1 END EOF SECOND LEVEL");
	}

	@Autowired
	TopLevelAnalysisForVoucher topLevelAnalysisForVoucher;

	@Test
	public void topLevelTestsForTOTALVoucherVALUE() {

		final String groupByPropName = Constants.PROP_MATERIAL_GROUP_4;
		final int dir = Constants.SORT_DIRECTION_ASCENDING;
		final Map<String, List<String>> argfilter = null;
		final int pgNum = 0;
		List<String> yyyymm = null;
		final String searchStr = null;
		final String orderByKPI = Constants.KPI_VV_VALUE;

//		kPIFilterAndGroupByHandler.getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, new HashMap<>(), false, searchStr);

		topLevelAnalysisForVoucher.getTotalVoucherValue(argfilter, yyyymm);
		logger.debug("1 END ..... --------------------------------------------------------------------------------------------------");

		yyyymm = new ArrayList<>();
		yyyymm.add("2022-07");
		topLevelAnalysisForVoucher.getTotalVoucherValue(argfilter, yyyymm);
		logger.debug("2 END....--------------------------------------------------------------------------------------------------");

		yyyymm = new ArrayList<>();
		yyyymm.add("2021-07");
		topLevelAnalysisForVoucher.getTotalVoucherValue(argfilter, yyyymm);

		logger.debug("3 END....--------------------------------------------------------------------------------------------------");

		yyyymm = new ArrayList<>();
		yyyymm.add("2025-07");
		topLevelAnalysisForVoucher.getTotalVoucherValue(argfilter, yyyymm);

		logger.debug("4 END....--------------------------------------------------------------------------------------------------");
	}

	@Test
	public void topLevelTestForTOTALVoucherRemaining() {

		final String groupByPropName = Constants.PROP_MATERIAL_GROUP_4;
		final int dir = Constants.SORT_DIRECTION_ASCENDING;
		final Map<String, List<String>> argfilter = null;
		final int pgNum = 0;
		List<String> yyyymm = null;
		final String searchStr = null;
		final String orderByKPI = Constants.KPI_VV_VALUE;

//		kPIFilterAndGroupByHandler.getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, new HashMap<>(), false, searchStr);

		topLevelAnalysisForVoucher.getTotalVoucherRemaining(argfilter, yyyymm);
		logger.debug("1 END ..... --------------------------------------------------------------------------------------------------");

		yyyymm = new ArrayList<>();
		yyyymm.add("2022-07");
		topLevelAnalysisForVoucher.getTotalVoucherRemaining(argfilter, yyyymm);
		logger.debug("2 END....--------------------------------------------------------------------------------------------------");

		yyyymm = new ArrayList<>();
		yyyymm.add("2021-07");
		topLevelAnalysisForVoucher.getTotalVoucherRemaining(argfilter, yyyymm);

		logger.debug("3 END....--------------------------------------------------------------------------------------------------");

		yyyymm = new ArrayList<>();
		yyyymm.add("2025-07");
		topLevelAnalysisForVoucher.getTotalVoucherRemaining(argfilter, yyyymm);

		logger.debug("4 END....--------------------------------------------------------------------------------------------------");
	}

	@Test
	public void topLevelTestForVRLast7Days() {

		final Map<String, List<String>> argfilter = null;
		topLevelAnalysisForVoucher.getTotalVoucherRemainingValueForLast7Days(argfilter);
		logger.debug("1 END ..... --------------------------------------------------------------------------------------------------");

	}
	
	@Test
	public void topLevelTestForVRLastONEYEAR() {

		final Map<String, List<String>> argfilter = null;
		topLevelAnalysisForVoucher.getTotalVoucherRemainingValueForLastONEYEAR(argfilter);
		logger.debug("1 END ..... --------------------------------------------------------------------------------------------------");

	}
	
	@Test
	public void topLevelTestForVTForASpecificDAY() {

		final Map<String, List<String>> argfilter = null;

//		kPIFilterAndGroupByHandler.getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, new HashMap<>(), false, searchStr);

		topLevelAnalysisForVoucher.getTotalVoucherValueForSpecificDate(argfilter, "2020-10-11");
		logger.debug("1 END ..... --------------------------------------------------------------------------------------------------");
	}

	
	@Test
	public void topLevelTestForVTForASpecificMONTH() {

		final Map<String, List<String>> argfilter = null;

//		kPIFilterAndGroupByHandler.getTotalVoucherValue(groupByPropName, dir, argfilter, pgNum, yyyymm, new HashMap<>(), false, searchStr);

		topLevelAnalysisForVoucher.getTotalVoucherValueForSpecificMonth(argfilter, "2019-05");
		logger.debug("1 END ..... --------------------------------------------------------------------------------------------------");
		topLevelAnalysisForVoucher.getTotalVoucherValueForSpecificMonth(argfilter, "2020-05");
		logger.debug("2 END ..... --------------------------------------------------------------------------------------------------");
		topLevelAnalysisForVoucher.getTotalVoucherValueForSpecificMonth(argfilter, "2021-05");
		logger.debug("3 END ..... --------------------------------------------------------------------------------------------------");
	}
	
	@Test
	public void topLevelTestForVVLast7Days() {

		final Map<String, List<String>> argfilter = null;
		topLevelAnalysisForVoucher.getTotalVoucherValueForLast7Days(argfilter);
		logger.debug(
				"1 END ..... --------------------------------------------------------------------------------------------------");

	}

	@Test
	public void topLevelTestForVVLastONEYEAR() {

		final Map<String, List<String>> argfilter = null;
		topLevelAnalysisForVoucher.getTotalVoucherValueForLastONEYEAR(argfilter);
		logger.debug(
				"1 END ..... --------------------------------------------------------------------------------------------------");

	}
	

}
