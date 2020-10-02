package com.vf.ana;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;

@SpringBootTest
public class ComboTests {

	@Autowired
	ComboRepositoryPASD comboRepositoryPASD;
	
	@Autowired
	PASDAnaRepository pASDAnaRepository;
	
	
	
	Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	public void getMeasuresFromPOIDbyDimension() {
		Map<String, Map<String, Double>> ret = comboRepositoryPASD.getAllMeasuresFromPOIDForAPropertyForAllPropValues("parentSupplierId");
		logger.debug(">>>>>POInv VALUES BY BY DIM{}", ret);
	}

	@Test
	public void getTotalOfAllMeasuresFromPOIDimension() {
		Map<String, Map<String, Double>> ret = comboRepositoryPASD.getTotalOfAllMeasuresFromPOID();
		logger.debug(">>>>>POInv TOTAL VALUES {}", ret);
	}

	
	@Test
	public void getAllMeasuresFromPASDimension() {
		Map<String, Map<String, Double>> ret = comboRepositoryPASD.getAllMeasuresFromPASDForAPropertyForAllPropValues("opcoCode");
		logger.debug(">>>>>SPN DETAILS VALUES By DIM {}", ret);		
	}

	@Test
	public void getTotalOfAllMeasuresFromPASD() {
		Map<String, Map<String, Double>> ret = comboRepositoryPASD.getTotalOfAllMeasuresFromPASD();
		logger.debug(">>>>>SPN DETAILS TOTAL VALUES {}", ret);		
	}

	@Test
	public void getAllMeasuresForFirstLevelPropValues() {
		Map<String, Map<String, Double>> ret = comboRepositoryPASD.getAllMeasuresForFirstLevelPropValues("supplierPartNumber", 
				1,  "invoiceValue", Constants.SORT_DIRECTION_DESCENDING);
		logger.debug(">>>>>123POInv VALUES BY BY DIM{}", ret);

//		ret = comboRepositoryPASD.getAllMeasuresForFirstLevelPropValues("supplierPartNumber");
//		logger.debug(">>>>>POInv VALUES BY BY DIM{}", ret);
	}
	
	
	
	//16-9
	@Test
	public void getCountOfAllItemsByPropValues() {
		Map<String, Integer> ret = comboRepositoryPASD.getCountOfAllItemsByPropValues(Constants.PROP_CATALOG_TYPE, 
				Constants.PROP_MATERIAL_GROUP_4,
				Constants.PROP_OPCO_CODE,
				Constants.PROP_PARENT_SUPPLIER_ID,
				Constants.PROP_PRICE_ACREEMENT_REFERENCE_NAME,
				Constants.PROP_SUPPLIER_PART_NUMBER,
				Constants.PROP_TRADING_MODEL);
		logger.debug("{}", ret);
	}


}
