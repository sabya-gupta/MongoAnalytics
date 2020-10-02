package com.vf.ana;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class Test2 {

	@Autowired
	PASDAnaRepository pASDAnaRepository;

	@Autowired
	DatePASDAnaRepository datePASDAnaRepository;

	Logger logger = LoggerFactory.getLogger(getClass());

//	@Test //NOT REQUIRED
//	public void getActiveOLAsByDate() {
//		String dt = "2020-09-01";
//		int num = pASDAnaRepository.getActiveOLAsByDate(dt);
//		logger.debug("ACTIVE OPEN OLAs AS ON {} - {}", dt, num);
//	}

	
//	@Test
//	public void getActivePAsByDate() {
//		String dt = "2020-09-01";
//		int num = pASDAnaRepository.getActivePAsByDate(dt, null);
//		logger.debug("ACTIVE OPEN **PAs AS ON {} - {}", dt, num);
//	}
//	

	@Test
	public void getActiveOlAsByDim() {
		pASDAnaRepository.getALLActiveOLAsByDim("parentSupplierId");
	}

	@Test
	public void getActivePAsByDim() {
		pASDAnaRepository.getALLActivePAsByDim("parentSupplierId");
	}

	@Test
	public void getAllActiveOlas() {
		int num = pASDAnaRepository.getNumberOfALLActiveOLAs();
		logger.debug("ALL ACTIVE OLAs {}", num);
	}
	
	@Test
	public void getAllActivePAs() {
		int num = pASDAnaRepository.getNumberOfALLActivePAs();
		logger.debug("ALL ACTIVE PAs {}", num);
	}
	
	
	@Test
	public void getAllActiveOlasByDateRange() {
		Map<String, Integer> retMap =  pASDAnaRepository.getActiveOLAsByDateRange("2020-09-01", "2020-09-06", null);
		System.out.println(retMap);
		Map<String, String> filter = new HashMap<String, String>();
		filter.put("materialGroupL4", "A212");
		filter.put("supplierId", "0400002261");
		retMap =  pASDAnaRepository.getActiveOLAsByDateRange("2020-09-01", "2020-09-06", null);
		System.out.println(retMap);
	}
	
	
	//START OF 20-09-2020
	@Test
	public void getActivePAsByFilterAndDateRange() {
		Map<String, Integer> retMap =  pASDAnaRepository.getActivePAsByFilterAndDateRange("2020-09-01", "2020-09-09", null);
		System.out.println(retMap);
		
		
		Map<String, String> filter = new HashMap<String, String>();
		filter.put("materialGroupL4", "A212");
		filter.put("supplierId", "0400002261");
		retMap =  pASDAnaRepository.getActivePAsByFilterAndDateRange("2020-09-01", "2020-09-09", filter);
		System.out.println(retMap);
	}
	
	
	@Test
	public void getActiveItemsByFilterAndDateRange() {
		Map<String, Integer> retMap =  pASDAnaRepository.getActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-09", null);
		System.out.println(retMap);
		
		
		Map<String, String> filter = new HashMap<String, String>();
		filter.put("materialGroupL4", "A212");
		filter.put("supplierId", "0400002261");
		retMap =  pASDAnaRepository.getActiveItemsByFilterAndDateRange("2020-09-01", "2020-09-09", filter);
		System.out.println(retMap);
	}
	
	// END OF 20-09-2020
	
	
	
	
	
	
	//9 SEPT
	
	
	@Test
	public void getTotallPAsByFilter() {
		
		HashMap<String, String> filterMap = new HashMap<String, String>();
		
		filterMap.put("parentSupplierId", "11796860");
		filterMap.put("materialGroupL4", "A218");
		int ov = pASDAnaRepository.getNumberOfALLActivePAsByFilter(filterMap, null);
		logger.debug("Total PA >>>>>> {}", ov);

		ov = 
		pASDAnaRepository.getNumberOfALLActivePAsByFilter(filterMap, "2018-07-10");
		logger.debug("Total PA is  {}", ov);
	}

	
	
	@Test
	public void getNumberOfALLActiveItems() {
		int ai = pASDAnaRepository.getNumberOfALLActiveItems();
		logger.debug("Total Number of Active Items are  {}", ai);
		
	}
	
	@Test
	public void getNumberOfALLActiveItemsByFilter() {
		HashMap<String, String> filterMap = new HashMap<String, String>();
		
		filterMap.put("parentSupplierId", "11796860");
		filterMap.put("materialGroupL4", "A218");
		int ai = pASDAnaRepository.getNumberOfALLActiveItemsByFilter(filterMap, null);
		logger.debug("Total Number of Active Items are 1 {}", ai);
		
		ai = pASDAnaRepository.getNumberOfALLActiveItemsByFilter(filterMap, "2018-07-10");
		logger.debug("Total Number of Active Items are 2>> {}", ai);
		
	}
	
	
//	@Test
//	public void getAllActiveOlasByDate() {
//		long t1 = System.currentTimeMillis();
//		datePASDAnaRepository.generateDatePASDNL();
//		logger.debug("{}", (System.currentTimeMillis()-t1));
//	}

}
