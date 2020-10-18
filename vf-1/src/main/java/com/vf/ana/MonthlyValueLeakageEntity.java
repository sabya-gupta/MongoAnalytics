package com.vf.ana;

import java.util.List;
import java.util.Map;

public class MonthlyValueLeakageEntity {


	private List<Map<String, String>> priceReferenceDetails;

	private Map<String, Map<String, Double>> leakagaesForPrevMonths;

	public List<Map<String, String>> getPriceReferenceDetails() {
		return priceReferenceDetails;
	}

	public void setPriceReferenceDetails(final List<Map<String, String>> priceReferenceDetails) {
		this.priceReferenceDetails = priceReferenceDetails;
	}

	public Map<String, Map<String, Double>> getLeakagaesForPrevMonths() {
		return leakagaesForPrevMonths;
	}

	public void setLeakagaesForPrevMonths(final Map<String, Map<String, Double>> lkgMap) {
		this.leakagaesForPrevMonths = lkgMap;
	}


}
