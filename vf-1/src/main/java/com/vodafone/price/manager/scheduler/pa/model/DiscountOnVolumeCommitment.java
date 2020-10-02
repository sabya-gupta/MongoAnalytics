/**
 * 
 */
package com.vodafone.price.manager.scheduler.pa.model;

import java.io.Serializable;

/**
 * @author Anup Kumar Gupta
 * @FileName DiscountOnVolumeCommitment.java
 * @date 2020-09-01
 */
public class DiscountOnVolumeCommitment implements Serializable {
	
	private static final long serialVersionUID = -5026833346097593330L;
	private String validFrom = null;
	private String validTo = null;
	private String discount = null;
	private String volumeCommitment = null;
	
	
	public DiscountOnVolumeCommitment() {}


	/**
	 * @param validFrom
	 * @param validTo
	 * @param discount
	 * @param volumeCommitment
	 */
	public DiscountOnVolumeCommitment(String validFrom, String validTo, String discount, String volumeCommitment) {
		super();
		this.validFrom = validFrom;
		this.validTo = validTo;
		this.discount = discount;
		this.volumeCommitment = volumeCommitment;
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
	 * @return the volumeCommitment
	 */
	public String getVolumeCommitment() {
		return volumeCommitment;
	}


	/**
	 * @param volumeCommitment the volumeCommitment to set
	 */
	public void setVolumeCommitment(String volumeCommitment) {
		this.volumeCommitment = volumeCommitment;
	}
	

	
	 
	
}
