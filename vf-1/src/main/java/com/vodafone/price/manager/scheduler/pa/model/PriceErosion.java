/**
 * 
 */
package com.vodafone.price.manager.scheduler.pa.model;

import java.io.Serializable;

/**
 * @author Anup Kumar Gupta
 * @FileName PriceErosion.java
 * @date 2020-09-01
 */
public class PriceErosion implements Serializable {

	private static final long serialVersionUID = 1897416063484214987L;
	private String discount = null;
	private String validFrom = null;
	private String validTo = null;
	
	/**
	 * @param discount
	 * @param validFrom
	 * @param validTo
	 */
	public PriceErosion(String discount, String validFrom, String validTo) {
		super();
		this.discount = discount;
		this.validFrom = validFrom;
		this.validTo = validTo;
	}

	public PriceErosion() {}

	/**
	 * @return the discount
	 */
	public String getDiscount() {
		return discount;
	}

	/**
	 * @param discount the discount to set
	 */
	public void setDiscount(String discount) {
		this.discount = discount;
	}

	/**
	 * @return the validFrom
	 */
	public String getValidFrom() {
		return validFrom;
	}

	/**
	 * @param validFrom the validFrom to set
	 */
	public void setValidFrom(String validFrom) {
		this.validFrom = validFrom;
	}

	/**
	 * @return the validTo
	 */
	public String getValidTo() {
		return validTo;
	}

	/**
	 * @param validTo the validTo to set
	 */
	public void setValidTo(String validTo) {
		this.validTo = validTo;
	}

	@Override
	public String toString() {
		return "PriceErosion [discount=" + discount + ", validFrom=" + validFrom + ", validTo=" + validTo + "]";
	}
	
	
	 
}
