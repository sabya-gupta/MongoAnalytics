package com.vf.ana.valueLeakageRecovered;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.vf.ana.MonthlyValueLeakageEntity;
import com.vf.ana.ValueLeakageAnalysis;

/*
 * 1. VL based for a given month.
 */
@Service
public class MonthlyValueLeakageByRefNum {

	@Autowired
	ValueLeakageAnalysis valueLeakageAnalysis; // Reusing this code with some changes. Please note page size negetive
	Logger logger = LoggerFactory.getLogger(getClass());

	public MonthlyValueLeakageEntity getValueLeakageForMonthBasedOnPRN(final LocalDate dt) {

		final YearMonth ym = YearMonth.from(dt);
		final List<String> ymLst = new ArrayList<>();
		ymLst.add(ym.toString());
		logger.debug(">>>>>>>>>FINDING FOR YYYYMM = {} ", ymLst);
		final MonthlyValueLeakageEntity ret = valueLeakageAnalysis.renderValueLeakageGrid(null, ymLst, null, 0, -1, 0,
				null, null, null);

		logger.debug("RET = {} ", ret.getPriceReferenceDetails());
		logger.debug("SIZE = {} ", ret.getLeakagaesForPrevMonths());
		logger.debug("------------------------------------------------------");

		return ret;
	}

}
