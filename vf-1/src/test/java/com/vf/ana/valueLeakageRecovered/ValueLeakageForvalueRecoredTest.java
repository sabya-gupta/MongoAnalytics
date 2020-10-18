package com.vf.ana.valueLeakageRecovered;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ValueLeakageForvalueRecoredTest {

	@Autowired
	MonthlyValueLeakageByRefNum monthlyValueLeakageByRefNum;

	@Test
	public void testMonthlyData() {
		LocalDate dt = LocalDate.now();
		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);

		dt = LocalDate.now().minusYears(1);
		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);

		dt = LocalDate.now().minusMonths(1);
		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);

		dt = LocalDate.now().plusMonths(10);
		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);

		dt = LocalDate.now().plusMonths(1);
		monthlyValueLeakageByRefNum.getValueLeakageForMonthBasedOnPRN(dt);

	}

}
