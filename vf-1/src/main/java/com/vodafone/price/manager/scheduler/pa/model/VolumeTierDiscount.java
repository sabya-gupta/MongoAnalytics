/**
 * 
 */
package com.vodafone.price.manager.scheduler.pa.model;

import java.io.Serializable;

/**
 * @author Anup Kumar Gupta
 * @FileName VolumeTierDiscount.java
 * @date 2020-09-01
 */
public class VolumeTierDiscount implements Serializable {
	
	private static final long serialVersionUID = -8331971790524527651L;
	private String openingOrderVolume= null;
    private String closingOrderVolume= null;
    private String discount= null;
    private String validFrom= null;
    private String validTo= null;
    
    public VolumeTierDiscount() {}
    
    
    
	/**
	 * @param openingOrderVolume
	 * @param closingOrderVolume
	 * @param discount
	 * @param validFrom
	 * @param validTo
	 */
	public VolumeTierDiscount(String openingOrderVolume, String closingOrderVolume, String discount, String validFrom, String validTo) {
		super();
		this.openingOrderVolume = openingOrderVolume;
		this.closingOrderVolume = closingOrderVolume;
		this.discount = discount;
		this.validFrom = validFrom;
		this.validTo = validTo;
	}



	/**
	 * @return the openingOrderVolume
	 */
	public String getOpeningOrderVolume() {
		return openingOrderVolume;
	}
	/**
	 * @param openingOrderVolume the openingOrderVolume to set
	 */
	public void setOpeningOrderVolume(String openingOrderVolume) {
		this.openingOrderVolume = openingOrderVolume;
	}
	/**
	 * @return the closingOrderVolume
	 */
	public String getClosingOrderVolume() {
		return closingOrderVolume;
	}
	/**
	 * @param closingOrderVolume the closingOrderVolume to set
	 */
	public void setClosingOrderVolume(String closingOrderVolume) {
		this.closingOrderVolume = closingOrderVolume;
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
	 
    
    
    
}
