package com.vf.ana;

import org.springframework.boot.test.context.SpringBootTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import com.vf.ana.PriceAgreementSpnDetailsRepository;

@SpringBootTest
public class PriceAgreementTest {
	
	@Autowired
	PriceAgreementSpnDetailsRepository priceAgreementSpnDetailsRepository;

	Logger logger = LoggerFactory.getLogger(getClass());
	
	@Test
	public void getAllLastOneYr () {
		priceAgreementSpnDetailsRepository.getAllLastOneYr();
		logger.debug ("DatewisePriceAgreement Collection created");
	}

}
