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
public class TestSPNbyPAR {
	@Autowired
	KPIFilterAndGroupByHandler kPIFilterAndGroupByHandler;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	public void getAllKPIWithFilter() {

		final String grpByPropName = Constants.PROP_SUPPLIER_PART_NUMBER;
		final String kpiToOrderBy = Constants.KPI_INVOICE_VALUE;

		final Map<String, List<String>> filter1 = new HashMap<>();

		final String paId = "VPC_BF_SV_Se_LU75_PMS_MULT_400074915_00C06A9829B3";
//		final String paId = "VPC_BF_IT&E_OR_LU53_SWR_MULT_400017770_2EFB2450711E";
//		final String paId = "VPC_BF_IT&E_OR_LU54_SWR_MULT_400017770_946F721F9836";

		final List<String> l1 = new ArrayList<>();
		l1.add(paId);
		filter1.put(Constants.PROP_PRICE_ACREEMENT_REFERENCE_ID, l1);

		final Map<String, Map<String, Double>> ret = kPIFilterAndGroupByHandler.getDataByProp(grpByPropName,
				kpiToOrderBy, Constants.SORT_DIRECTION_DESCENDING, filter1, 0, null, null);
		logger.debug("Final result : {} - nItems = {}", ret, ret.size());

		final int num = kPIFilterAndGroupByHandler.getDataCOUNTByProp(grpByPropName, kpiToOrderBy,
				Constants.SORT_DIRECTION_DESCENDING, filter1, null, null);
		logger.debug("COUNT nItems = {}", num);

	}

}
