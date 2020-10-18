package com.vf.ana;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.vf.ana.valueLeakageRecovered.MonthlyValueLeakageByRefNum;

@SpringBootTest
public class ValueLeakageAnalysisTests {
	
	@Autowired
	ValueLeakageAnalysis valueLeakageAnalysis;

	Logger logger = LoggerFactory.getLogger(getClass());
	
	@Test // This is for value leakage grid from valueLeakageRecovered collection
	public void rederGridNew() {
		final List<List<Double>> valRangeFilters = new ArrayList<>();
		String searchStr=null;
		String searchField=null;
		
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, null, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 1-----------------");
		final List<String> yyyymm = new ArrayList<>();
		yyyymm.add("2020-01");
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 2-----------------");
		yyyymm.add("2020-10");
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 3-----------------");

		final Map<String, List<String>> filter = new HashMap<>();
		final List<String> L4List = new ArrayList<>();
		L4List.add("K208");
		filter.put("materialGroupL4", L4List);
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(filter, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(filter, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 4-----------------");

		L4List.add("K101");
		filter.put("materialGroupL4", L4List);
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(filter, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(filter, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 5-----------------");

		List<String> pp = new ArrayList<>();
		pp.add("Invoice Date");
		filter.put("priceReference", pp);
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(filter, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(filter, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 6-----------------");

		pp = new ArrayList<>();
		pp.add("Should give 0");
		filter.put("priceReference", pp);
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(filter, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(filter, yyyymm, null, Constants.SORT_DIRECTION_ASCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 7-----------------");

		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, null, "supplierName", Constants.SORT_DIRECTION_ASCENDING,
				0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 8-----------------");

		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, null, "supplierName", Constants.SORT_DIRECTION_DESCENDING,
				0, 5, valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 9-----------------");

		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		final List<Double> vals = new ArrayList<>();
		vals.add(400000.00);
		valRangeFilters.add(vals);
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 10-----------------");

		vals.add(500000.00);
		valRangeFilters.add(vals);
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 11-----------------");

		final List<Double> vals2 = new ArrayList<>();
		vals2.add(0.00);
		vals2.add(50000.00);
		valRangeFilters.add(vals2);
		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 12-----------------");

		searchStr="cle";
		searchField="supplierName";

		valueLeakageAnalysis.getCountOfValueLeakageGridNew(null, null, valRangeFilters, searchField, searchStr);
		valueLeakageAnalysis.renderValueLeakageGridNew(null, null, null, Constants.SORT_DIRECTION_DESCENDING, 0, 5,
				valRangeFilters, searchField, searchStr);
		logger.debug("------------------End of 13-----------------");

	}
	
	@Autowired
	MonthlyValueLeakageByRefNum monthlyValueLeakageByRefNum;

	@Test // This test is to give value leakage per Purchase Reference given a date from
			// the ValueLeakageInvoicePurchaseOrder collection
	public void testMonthlyData() {
		final LocalDate dt = LocalDate.now();
		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);

//		dt = LocalDate.now().minusYears(1);
//		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);
//
//		dt = LocalDate.now().minusMonths(1);
//		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);
//
//		dt = LocalDate.now().plusMonths(10);
//		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);
//
//		dt = LocalDate.now().plusMonths(1);
//		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);

	}

	@Test // This is Value Leakage monthwise and total which is shown in the pop-up for a given price ref
	public void getMonthwiseValueLeakageForAPriceReferenceForPOPUP() {
		valueLeakageAnalysis
				.getMonthWiseValueLeakageForAPriceRef("VPC_BF_IT&E_OR_LU56_SWR_MULT_400017770_CDA6E303643F");
	}
	
	@Test // This is Value RECOVERED for a given month for given price ref shown in the popup
	public void getvalueRECOVEREDForAMonthForAPriceReference_ForPOPUP() {
		valueLeakageAnalysis
				.getValueRECOVEREDForAPriceRefForAMonth("VPC_BF_IT&E_OR_LU56_SWR_MULT_400017770_CDA6E303643F",
						"2020-09");

		valueLeakageAnalysis.getValueRECOVEREDForAPriceRefForAMonth(
				"VPC_BF_IT&E_OR_LU56_SWR_MULT_400017770_CDA6E303643F", "2020-10");

	}
	

}
