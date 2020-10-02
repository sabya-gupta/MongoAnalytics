package com.vf.ana;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class AggBatchFirstLevelTest {
	
	@Autowired
	AggBatchFirstLevel aggBatchFirstLevel;
	
	@Autowired
	AggBatchSecondLevel aggBatchSecondLevel;

	Logger logger = LoggerFactory.getLogger(getClass());
	
	@Test
	public void generateAggregatedCollection() {
		aggBatchFirstLevel.generateAggregatedCollection();
	}
	
	@Test
	public void generateAggregatedCollection2() {
		aggBatchSecondLevel.generateAggregatedCollection();
	}	
	
}
