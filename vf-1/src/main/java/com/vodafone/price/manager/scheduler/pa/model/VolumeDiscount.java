/**
 * 
 */
package com.vodafone.price.manager.scheduler.pa.model;

import java.io.Serializable;

/**
 * @author Anup Kumar Gupta
 * @FileName VolumeDiscount.java
 * @date 2020-09-01
 */
public class VolumeDiscount  implements Serializable {
	
 
	private static final long serialVersionUID = 8278568660801278491L;
	private String orderVolume= null;
    private String discount= null;
    private String validFrom= null;
    private String validTo= null;
    
    
    public VolumeDiscount() {}
    
    
	/**
	 * @param orderVolume
	 * @param discount
	 * @param validFrom
	 * @param validTo
	 */
	public VolumeDiscount(String orderVolume, String discount, String validFrom, String validTo) {
		super();
		this.orderVolume = orderVolume;
		this.discount = discount;
		this.validFrom = validFrom;
		this.validTo = validTo;
	}


	/**
	 * @return the orderVolume
	 */
	public String getOrderVolume() {
		return orderVolume;
	}
	/**
	 * @param orderVolume the orderVolume to set
	 */
	public void setOrderVolume(String orderVolume) {
		this.orderVolume = orderVolume;
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
