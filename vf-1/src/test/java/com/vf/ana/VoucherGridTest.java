package com.vf.ana;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest
public class VoucherGridTest {

	
	@Autowired
	VoucherAnalysis voucherAnalysis;

	Logger logger = LoggerFactory.getLogger(getClass());
	
	@Test
	public void topLevelTestsForVR() {
		
		String voucherId = "EAC49092C32A45F5A95B85049A51472B"; //"135F7C20E2184E2A8C479C029B29F15E";
		
		voucherAnalysis.renderVoucherGrid(voucherId);
		voucherAnalysis.getVoucherDetails(voucherId);

	}	

	
}
