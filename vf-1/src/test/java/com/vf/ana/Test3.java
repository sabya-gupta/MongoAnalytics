package com.vf.ana;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class Test3 {

	@Autowired
	PriceAgreementSpnDetailsRepository priceAgreementSpnDetailsRepository;

	Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	public void getTotalOV() {
		double ov = priceAgreementSpnDetailsRepository.getAllOrderValueIssued();
		logger.debug("Total OV issued is {}", ov);
	}

	@Test
	public void getTotalIV() {
		double ov = priceAgreementSpnDetailsRepository.getAllInvoiceValueIssued();
		logger.debug("Total Invoice issued is {}", ov);
	}


	


	@Test
	public void getTotalIVByDim() {
		Map<String, Double> ret = priceAgreementSpnDetailsRepository
				.getAllInvoiceValueIssuedByDimension("supplierName");
		logger.debug("IV {}", ret);
	}

	@Test
	public void getTotalOVByDim() {
		Map<String, Double> ret = priceAgreementSpnDetailsRepository
				.getAllOrderValueIssuedByDimension("supplierName");
		logger.debug("OV {}", ret);
	}
	
	@Test
	public void getAllOrderValueIssuedByDateRange() {//FFFFF
		Map<String, Double> ret = priceAgreementSpnDetailsRepository
				.getAllOrderValueIssuedByDateRange("2020-09-01", "2020-09-09");
		logger.debug("OV DR {}", ret);
	}
	
	@Test
	public void getAllInvoiceValueIssuedByDateRange() {
		Map<String, Double> ret = priceAgreementSpnDetailsRepository
				.getAllInvoiceValueIssuedByDateRange("2020-09-01", "2020-09-09");
		logger.debug("IV DR {}", ret);
	}
	
//	@Test
//	public void getAllInvoiceValueIssuedByDateRange2() {
//		Map<String, Double> ret = priceAgreementSpnDetailsRepository
//				.getAllInvoiceValueIssuedByDateRange2("2019-06-01", "2019-06-10");
//		logger.debug("IV DR 2 {}", ret);
//	}
	
	@Test
	public void getTotallOVByFilter() {
		
		HashMap<String, String> filterMap = new HashMap<String, String>();
		
		filterMap.put("supplierName", "ORACLE CORPORATION UK LIMITED");
		filterMap.put("matCatCodeL4", "K208");
		double ov = priceAgreementSpnDetailsRepository.getAllOrderValueIssuedByFilter(filterMap, null);
		logger.debug(">Total Order issued to RED HAT LIMITED is {}", ov);

		ov = priceAgreementSpnDetailsRepository.getAllOrderValueIssuedByFilter(filterMap, "2019-06-09");
		logger.debug(">>>Total Order issued to RED HAT LIMITED is {}", ov);
	}

	
	@Test
	public void getTotalIVByFilter() {
		
		HashMap<String, String> filterMap = new HashMap<String, String>();
		
		filterMap.put("supplierName", "ORACLE CORPORATION UK LIMITED");
		filterMap.put("matCatCodeL4", "K208");
		double ov = priceAgreementSpnDetailsRepository.getAllInvoiceValueIssuedByFilter(filterMap, null);
		logger.debug("Total Invoice issued to RED HAT LIMITED is {}", ov);

		ov = priceAgreementSpnDetailsRepository.getAllInvoiceValueIssuedByFilter(filterMap, "2019-06-26");
		logger.debug("Total Invoice issued to RED HAT LIMITED is {}", ov);
	}
	
	
	
	
	//9 Sept
	
	@Test
	public void getTotalNumberOfOrders() {
		int num = priceAgreementSpnDetailsRepository.getTotalNumberOfOrders();
		logger.debug("Total Number of orders is {}", num);
	}
	
	
	@Test
	public void getTotalNumberOfOrdersByDateRange() {
		Map<String, Integer> ret = priceAgreementSpnDetailsRepository.getTotalNumberOfOrdersByDateRange("2020-09-01", "2020-09-10");
		logger.debug("Total Number of orders is {}", ret);
	}
	
	
	
	@Test
	public void getTotalNumberOfOrdersByFilters() {
		HashMap<String, String> filterMap = new HashMap<String, String>();
		
		filterMap.put("supplierName", "ORACLE CORPORATION UK LIMITED");
		filterMap.put("matCatCodeL4", "K208");
		int ov = priceAgreementSpnDetailsRepository.getTotalNumberOfOrdersByFilters(filterMap, null);
		logger.debug("Total Number Order issued to RED HAT LIMITED is {}", ov);

		ov = priceAgreementSpnDetailsRepository.getTotalNumberOfOrdersByFilters(filterMap, "2019-06-09");
		logger.debug("Total number of Order issued to RED HAT LIMITED is {}", ov);

	}
}
