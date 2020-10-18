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
		
		final Map<String, List<String>> filters = new HashMap<>();
		
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ORDER_VALUE, filters);
		
		final List<String> suppIds = new ArrayList<>();
//		suppIds.add("xxxxxxxxxxxxxxxxxx");
		suppIds.add("0400074915");
		filters.put("supplierId", suppIds);
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ORDER_VALUE, filters);
		
	}
	
	@Test
	void testInvoiceValue() {
		
		logger.debug("----------iv----------------");
		final Map<String, List<String>> filters = new HashMap<>();
		
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.INVOICE_VALUE, filters);
		
		final List<String> suppIds = new ArrayList<>();
//		suppIds.add("xxxxxxxxxxxxxxxxxx");
		suppIds.add("0400074915");
		filters.put("supplierId", suppIds);
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.INVOICE_VALUE, filters);
		
	}
	
	@Test
	void testActiveItems() {
		logger.debug("----------AI----------------");
		
		final Map<String, List<String>> filters = new HashMap<>();
		
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ACTIVE_ITEMS, filters);
		
		final List<String> suppIds = new ArrayList<>();
//		suppIds.add("xxxxxxxxxxxxxxxxxx");
		suppIds.add("0400074915");
		filters.put("supplierId", suppIds);
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ACTIVE_ITEMS, filters);
		
	}
	
	@Test
	void testActivePAs() {
		
		logger.debug("----------PA----------------");
		final Map<String, List<String>> filters = new HashMap<>();
		
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ACTIVE_PRICE_AGREEMENT, filters);
		
		final List<String> suppIds = new ArrayList<>();
//		suppIds.add("xxxxxxxxxxxxxxxxxx");
		suppIds.add("0400074915");
		filters.put("supplierId", suppIds);
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.ACTIVE_PRICE_AGREEMENT, filters);
		
	}

	@Test
	void testOrderIssued() {
		logger.debug("----------ov----------------");

		final Map<String, List<String>> filters = new HashMap<>();

		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.NUMBER_OF_ORDERS, filters);

		final List<String> suppIds = new ArrayList<>();
//		suppIds.add("xxxxxxxxxxxxxxxxxx");
		suppIds.add("0400074915");
		filters.put("supplierId", suppIds);
		monthWiseAggregationLast1YrTopLevel.aggregateMonthWiseForLastOneYear(Constants.NUMBER_OF_ORDERS, filters);

	}
	
}
