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
public class ValueLeakageAnalysisTests {
	
	@Autowired
	ValueLeakageAnalysis valueLeakageAnalysis;

	Logger logger = LoggerFactory.getLogger(getClass());
	
	@Test
	public void rederGrid() {
		List<List<Double>> valRangeFilters = new ArrayList<>();
		String searchStr=null;
		String searchField=null;
		
		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, null, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 1-----------------");
		List<String> yyyymm = new ArrayList<String>();
		yyyymm.add("2020-01");
		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 2-----------------");
		yyyymm.add("2020-10");
		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 3-----------------");

		Map<String, List<String>> filter = new HashMap<String, List<String>>();
		List<String> L4List = new ArrayList<String>();
		L4List.add("K208");
		filter.put("materialGroupL4", L4List);
		valueLeakageAnalysis.getCountOfValueLeakageGrid(filter, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(filter, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 4-----------------");

		L4List.add("K101");
		filter.put("materialGroupL4", L4List);
		valueLeakageAnalysis.getCountOfValueLeakageGrid(filter, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(filter, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 5-----------------");

		List<String> pp = new ArrayList<String>();
		pp.add("Invoice Date");
		filter.put("priceReference", pp);
		valueLeakageAnalysis.getCountOfValueLeakageGrid(filter, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(filter, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 6-----------------");

		pp = new ArrayList<String>();
		pp.add("Should give 0");
		filter.put("priceReference", pp);
		valueLeakageAnalysis.getCountOfValueLeakageGrid(filter, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(filter, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 7-----------------");

		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, null, "supplierName", Constants.SORT_DIRECTION_ASCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 8-----------------");

		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, null, "supplierName", Constants.SORT_DIRECTION_DESCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 9-----------------");

		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		List<Double> vals = new ArrayList<Double>();
		vals.add(400000.00);
		valRangeFilters.add(vals);
		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 10-----------------");

		vals.add(500000.00);
		valRangeFilters.add(vals);
		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 11-----------------");

		List<Double> vals2 = new ArrayList<Double>();
		vals2.add(0.00);
		vals2.add(50000.00);
		valRangeFilters.add(vals2);
		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 12-----------------");

		searchStr="cle";
		searchField="supplierName";

		valueLeakageAnalysis.getCountOfValueLeakageGrid(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGrid(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 13-----------------");

	}
	
}
