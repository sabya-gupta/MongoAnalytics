package com.vf.ana;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class SPNGridTest {

	@Autowired
	SPNValidityGrid sPNValidityGrid;
	
	Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	public void test() {
		
		int pgSz=100;
		int pgNo = 0;
		Map<String, String> filterMap = new HashMap<>();
		int daysToExpire = 1000;
		
		sPNValidityGrid.getSPNValidityGrid(daysToExpire, pgSz, pgNo, filterMap, "diff", Constants.SORT_DIRECTION_ASCENDING);
		sPNValidityGrid.getSPNValidityGridCOUNT(daysToExpire, filterMap);
		
		filterMap.put("supplierName", "cle");
		filterMap.put("materialShortDesc", "k");
		sPNValidityGrid.getSPNValidityGrid(daysToExpire, pgSz, pgNo, filterMap, "diff", Constants.SORT_DIRECTION_ASCENDING);
		sPNValidityGrid.getSPNValidityGridCOUNT(daysToExpire, filterMap);
		
		filterMap.put("supplierName", "cle");
		filterMap.put("materialShortDesc", "k");
		sPNValidityGrid.getSPNValidityGrid(daysToExpire, pgSz, pgNo, filterMap, "diff", Constants.SORT_DIRECTION_DESCENDING);
		sPNValidityGrid.getSPNValidityGridCOUNT(daysToExpire, filterMap);
		
		daysToExpire = 1000;
		filterMap.clear();
		filterMap.put("supplierName", "sed");
//		filterMap.put("materialShortDesc", "k");
		sPNValidityGrid.getSPNValidityGrid(daysToExpire, pgSz, pgNo, filterMap, "diff", Constants.SORT_DIRECTION_DESCENDING);
		sPNValidityGrid.getSPNValidityGridCOUNT(daysToExpire, filterMap);
		
		
		filterMap.clear();
		logger.debug("-----------------------------------------------------------------------------------------------");
		sPNValidityGrid.getSPNValidityGrid(daysToExpire, pgSz, pgNo, filterMap, "diff", Constants.SORT_DIRECTION_DESCENDING);
		sPNValidityGrid.getSPNValidityGridCOUNT(daysToExpire, filterMap);
		daysToExpire = 300;
		sPNValidityGrid.getSPNValidityGrid(daysToExpire, pgSz, pgNo, filterMap, "diff", Constants.SORT_DIRECTION_DESCENDING);
		sPNValidityGrid.getSPNValidityGridCOUNT(daysToExpire, filterMap);
	}

}
