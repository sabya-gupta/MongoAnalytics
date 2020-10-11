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
public class MonthWiseAggregationLast1YrTopLevelTests {

	@Autowired
	MonthWiseAggregationLast1YrTopLevel monthWiseAggregationLast1YrTopLevel;
	
	Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	void testOrderValue() {
		logger.debug("----------ov----------------");
		
		Map<String, List<String>> filters = new HashMap<>();
		
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ORDER_VALUE, filters);
		
		List<String> suppIds = new ArrayList<String>();
//		suppIds.add("xxxxxxxxxxxxxxxxxx");
		suppIds.add("0400074915");
		filters.put("supplierId", suppIds);
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ORDER_VALUE, filters);
		
	}
	
	@Test
	void testInvoiceValue() {
		
		logger.debug("----------iv----------------");
		Map<String, List<String>> filters = new HashMap<>();
		
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.INVOICE_VALUE, filters);
		
		List<String> suppIds = new ArrayList<String>();
//		suppIds.add("xxxxxxxxxxxxxxxxxx");
		suppIds.add("0400074915");
		filters.put("supplierId", suppIds);
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.INVOICE_VALUE, filters);
		
	}
	
	@Test
	void testActiveItems() {
		logger.debug("----------AI----------------");
		
		Map<String, List<String>> filters = new HashMap<>();
		
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ACTIVE_ITEMS, filters);
		
		List<String> suppIds = new ArrayList<String>();
//		suppIds.add("xxxxxxxxxxxxxxxxxx");
		suppIds.add("0400074915");
		filters.put("supplierId", suppIds);
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ACTIVE_ITEMS, filters);
		
	}
	
	@Test
	void testActivePAs() {
		
		logger.debug("----------PA----------------");
		Map<String, List<String>> filters = new HashMap<>();
		
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ACTIVE_PRICE_AGREEMENT, filters);
		
		List<String> suppIds = new ArrayList<String>();
//		suppIds.add("xxxxxxxxxxxxxxxxxx");
		suppIds.add("0400074915");
		filters.put("supplierId", suppIds);
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ACTIVE_PRICE_AGREEMENT, filters);
		
	}
	
}
