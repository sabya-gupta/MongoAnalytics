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
public class ValueRecoveredDashBoardTest {

	@Autowired
	KPIFilterAndGroupByHandler kPIFilterAndGroupByHandler;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Test // TEST FOR SECOND LEVEL. NOTE: SEE ALL CHANGES IN THE FILE
			// KPIFilterAndGroupByHandler
	public void getAllKPIWithFilter() {

		final String grpByPropName = Constants.PROP_MATERIAL_GROUP_4; // materialGroupL4
		final String kpiToOrderBy = Constants.KPI_VLREC_VALUE;

		
		final Map<String, List<String>> filter1 = new HashMap<>();

		final List<String> l1 = new ArrayList<>();
//		l1.add("K208");
//		l1.add("K209");
//		filter1.put(Constants.PROP_MATERIAL_GROUP_4, l1);

//		List<String> lstSearch = new ArrayList<String>();
//		lstSearch.add("0400002261");
//		filter1.put(grpByPropName, lstSearch);
		
		final List<String> lstSearch = new ArrayList<>();
		lstSearch.add("Buy From");
		filter1.put(Constants.PROP_TRADING_MODEL, lstSearch);
				
		final List<String> dates = new ArrayList<>();
		dates.add("1800-05");
		dates.add("2020-09");
		
		
		final String searchStr = null; //"bbb"; //"80";
		
		
		final Map<String, List<String>> filter = filter1;
		
		final Map<String, Map<String, Double>> ret = kPIFilterAndGroupByHandler.getDataByProp(grpByPropName,
				kpiToOrderBy, Constants.SORT_DIRECTION_DESCENDING, null, 0, null, searchStr);
		logger.debug("Final result : {} - nItems = {}", ret, ret.size());
		
		final int cnt = kPIFilterAndGroupByHandler.getDataCOUNTByProp(grpByPropName, kpiToOrderBy,
				Constants.SORT_DIRECTION_DESCENDING, null, null, null);
		logger.debug("Final result : {}", cnt);

	}

	@Autowired
	TopLevelAnalysisForLeakageRecoved topLevelAnalysisForLeakageRecoved;

	@Test // TEST FOR TOP LEVEL
	public void getTotalLeakageRecovered() {
		final Map<String, List<String>> filter1 = new HashMap<>();
		final List<String> dates = new ArrayList<>();
		dates.add("2020-10");

		final List<String> lstSearch = new ArrayList<>();
		lstSearch.add("K205");
		filter1.put(Constants.PROP_MATERIAL_GROUP_4, lstSearch);

		final List<String> l1 = new ArrayList<>();
		topLevelAnalysisForLeakageRecoved.getTotalRecoveredValue(filter1, dates);
	}

	@Test // TEST FOR LAST ONE YEAR
	public void getTotalLeakageRecoveredMONTHWISE_LAST_ONE_YEAR() {
		final Map<String, List<String>> filter1 = new HashMap<>();

		topLevelAnalysisForLeakageRecoved.getMONTH_WISERecoveredValueForLastONEYEAR(null);

		List<String> lstSearch = new ArrayList<>();
		lstSearch.add("K205");
		filter1.put(Constants.PROP_MATERIAL_GROUP_4, lstSearch);

		topLevelAnalysisForLeakageRecoved.getMONTH_WISERecoveredValueForLastONEYEAR(filter1);

		lstSearch = new ArrayList<>();
		lstSearch.add("K206");
		filter1.put(Constants.PROP_MATERIAL_GROUP_4, lstSearch);

		topLevelAnalysisForLeakageRecoved.getMONTH_WISERecoveredValueForLastONEYEAR(filter1);

		lstSearch = new ArrayList<>();
		lstSearch.add("K206");
		lstSearch.add("K205");
		filter1.put(Constants.PROP_MATERIAL_GROUP_4, lstSearch);

		topLevelAnalysisForLeakageRecoved.getMONTH_WISERecoveredValueForLastONEYEAR(filter1);

	}

}
