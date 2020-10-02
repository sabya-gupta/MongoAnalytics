/**
 * 
 */
package com.vf.ana.ent;

import java.io.Serializable;
import java.util.List;

import com.vodafone.price.manager.scheduler.pa.model.DiscountOnVolumeCommitment;
import com.vodafone.price.manager.scheduler.pa.model.PriceErosion;
import com.vodafone.price.manager.scheduler.pa.model.VolumeDiscount;
import com.vodafone.price.manager.scheduler.pa.model.VolumeTierDiscount;

/**
 * @author Anup Kumar Gupta
 * @FileName PriceMechanism.java
 * @date 2020-09-01
 */
public class PriceMechanism implements Serializable {
	private static final long serialVersionUID = -3037723897409174312L;
	
	private List<PriceErosion> priceErosionDetails = null;
	private List<VolumeDiscount> volumeDiscounts = null;
	private List<VolumeTierDiscount> volumeTierDiscounts = null;
	private List<DiscountOnVolumeCommitment> discountOnVolumeCommitment = null;
	
	public PriceMechanism() {
		
	}
	/**
	 * @param priceErosionDetails
	 * @param volumeDiscounts
	 * @param volumeTierDiscounts
	 * @param discountOnVolumeCommitment
	 */
	public PriceMechanism(List<PriceErosion> priceErosionDetails, List<VolumeDiscount> volumeDiscounts,
			List<VolumeTierDiscount> volumeTierDiscounts, List<DiscountOnVolumeCommitment> discountOnVolumeCommitment) {
		super();
		this.priceErosionDetails = priceErosionDetails;
		this.volumeDiscounts = volumeDiscounts;
		this.volumeTierDiscounts = volumeTierDiscounts;
		this.discountOnVolumeCommitment = discountOnVolumeCommitment;
	}

	/**
	 * @return the priceErosionDetails
	 */
	public List<PriceErosion> getPriceErosionDetails() {
		return priceErosionDetails;
	}

	/**
	 * @param priceErosionDetails the priceErosionDetails to set
	 */
	public void setPriceErosionDetails(List<PriceErosion> priceErosionDetails) {
		this.priceErosionDetails = priceErosionDetails;
	}

	/**
	 * @return the volumeDiscounts
	 */
	public List<VolumeDiscount> getVolumeDiscounts() {
		return volumeDiscounts;
	}

	/**
	 * @param volumeDiscounts the volumeDiscounts to set
	 */
	public void setVolumeDiscounts(List<VolumeDiscount> volumeDiscounts) {
		this.volumeDiscounts = volumeDiscounts;
	}

	/**
	 * @return the volumeTierDiscounts
	 */
	public List<VolumeTierDiscount> getVolumeTierDiscounts() {
		return volumeTierDiscounts;
	}

	/**
	 * @param volumeTierDiscounts the volumeTierDiscounts to set
	 */
	public void setVolumeTierDiscounts(List<VolumeTierDiscount> volumeTierDiscounts) {
		this.volumeTierDiscounts = volumeTierDiscounts;
	}

	/**
	 * @return the discountOnVolumeCommitment
	 */
	public List<DiscountOnVolumeCommitment> getDiscountOnVolumeCommitment() {
		return discountOnVolumeCommitment;
	}

	/**
	 * @param discountOnVolumeCommitment the discountOnVolumeCommitment to set
	 */
	public void setDiscountOnVolumeCommitment(List<DiscountOnVolumeCommitment> discountOnVolumeCommitment) {
		this.discountOnVolumeCommitment = discountOnVolumeCommitment;
	}

}
