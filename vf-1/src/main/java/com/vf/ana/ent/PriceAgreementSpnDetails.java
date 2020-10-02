/**
 * 
 */
package com.vf.ana.ent;

import java.io.Serializable;
import java.time.LocalDate;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author Anup Kumar Gupta
 * @FileName PriceAgreementSpnDetails.java
 * @date 2020-09-01
 */
@Document(collection = "priceAgreementSpnDetails")
public class PriceAgreementSpnDetails implements Serializable {

	private static final long serialVersionUID = 3661527077018651602L;
	@Id
	private String priceAgreementSpnDetailsId="";
	private String parentSupplierId;
	private String supplierId = null;
	private String outlineAgreementNumber = null;
	private String catalogueType = null;//L1
	private String catalogueName = null;
	private String opcoCode = null;
	public String getParentSupplierId() {
		return parentSupplierId;
	}

	public void setParentSupplierId(String parentSupplierId) {
		this.parentSupplierId = parentSupplierId;
	}

	private String priceAgreementStatus = null;
	
	
	
	private LocalDate validFromDate = null;
	private LocalDate validToDate = null;
	private String netPrice = null;
	private String quantity = null;
	private PriceMechanism priceMechanism = null;
	private String materialGroupL4 = null;
	private String priceReference = null;
	private String priceUnit = null;
	private String materialShortDesc = null;
	private String supplierPartNumber = null;

	public PriceAgreementSpnDetails() {
		super();
	}

	/**
	 * @param validFromDate
	 * @param validToDate
	 * @param netPrice
	 * @param quantity
	 * @param priceMechanism
	 * @param materialGroupL4
	 * @param priceReference
	 * @param priceUnit
	 * @param materialShortDesc
	 * @param supplierPartNumber
	 */
//	public PriceAgreementSpnDetails(String validFromDate, String validToDate, String netPrice, String quantity,
//			PriceMechanism priceMechanism, String materialGroupL4, String priceReference, String priceUnit,
//			String materialShortDesc, String supplierPartNumber) {
//		super();
//		this.validFromDate = validFromDate;
//		this.validToDate = validToDate;
//		this.netPrice = netPrice;
//		this.quantity = quantity;
//		this.priceMechanism = priceMechanism;
//		this.materialGroupL4 = materialGroupL4;
//		this.priceReference = priceReference;
//		this.priceUnit = priceUnit;
//		this.materialShortDesc = materialShortDesc;
//		this.supplierPartNumber = supplierPartNumber;
//	}

	/**
	 * @param supplierId
	 * @param outlineAgreementNumber
	 * @param catalogueType
	 * @param catalogueName
	 * @param opcoCode
	 * @param priceAgreementStatus
	 */
	public PriceAgreementSpnDetails(String supplierId, String outlineAgreementNumber, String catalogueType,
			String catalogueName, String opcoCode, String priceAgreementStatus) {
		super();
		this.supplierId = supplierId;
		this.outlineAgreementNumber = outlineAgreementNumber;
		this.catalogueType = catalogueType;
		this.catalogueName = catalogueName;
		this.opcoCode = opcoCode;
		this.priceAgreementStatus = priceAgreementStatus;
	}

	/**
	 * @return the supplierPartNumber
	 */
	public String getSupplierPartNumber() {
		return supplierPartNumber;
	}

	/**
	 * @param supplierPartNumber the supplierPartNumber to set
	 */
	public void setSupplierPartNumber(String supplierPartNumber) {
		this.supplierPartNumber = supplierPartNumber;
	}

	/**
	 * @return the outlineAgreementNumber
	 */
	public String getOutlineAgreementNumber() {
		return outlineAgreementNumber;
	}

	/**
	 * @param outlineAgreementNumber the outlineAgreementNumber to set
	 */
	public void setOutlineAgreementNumber(String outlineAgreementNumber) {
		this.outlineAgreementNumber = outlineAgreementNumber;
	}

	/**
	 * @return the opcoCode
	 */
	public String getOpcoCode() {
		return opcoCode;
	}

	/**
	 * @param opcoCode the opcoCode to set
	 */
	public void setOpcoCode(String opcoCode) {
		this.opcoCode = opcoCode;
	}

	/**
	 * @return the validFromDate
	 */
	public LocalDate getValidFromDate() {
		return validFromDate;
	}

	/**
	 * @param validFromDate the validFromDate to set
	 */
	public void setValidFromDate(LocalDate validFromDate) {
		this.validFromDate = validFromDate;
	}

	/**
	 * @return the validToDate
	 */
	public LocalDate getValidToDate() {
		return validToDate;
	}

	/**
	 * @param validToDate the validToDate to set
	 */
	public void setValidToDate(LocalDate validToDate) {
		this.validToDate = validToDate;
	}

	/**
	 * @return the netPrice
	 */
	public String getNetPrice() {
		return netPrice;
	}

	/**
	 * @param netPrice the netPrice to set
	 */
	public void setNetPrice(String netPrice) {
		this.netPrice = netPrice;
	}

	/**
	 * @return the quantity
	 */
	public String getQuantity() {
		return quantity;
	}

	/**
	 * @param quantity the quantity to set
	 */
	public void setQuantity(String quantity) {
		this.quantity = quantity;
	}

	/**
	 * @return the priceMechanism
	 */
	public PriceMechanism getPriceMechanism() {
		return priceMechanism;
	}

	/**
	 * @param priceMechanism the priceMechanism to set
	 */
	public void setPriceMechanism(PriceMechanism priceMechanism) {
		this.priceMechanism = priceMechanism;
	}

	/**
	 * @return the materialGroupL4
	 */
	public String getMaterialGroupL4() {
		return materialGroupL4;
	}

	/**
	 * @param materialGroupL4 the materialGroupL4 to set
	 */
	public void setMaterialGroupL4(String materialGroupL4) {
		this.materialGroupL4 = materialGroupL4;
	}

	/**
	 * @return the priceReference
	 */
	public String getPriceReference() {
		return priceReference;
	}

	/**
	 * @param priceReference the priceReference to set
	 */
	public void setPriceReference(String priceReference) {
		this.priceReference = priceReference;
	}

	/**
	 * @return the priceUnit
	 */
	public String getPriceUnit() {
		return priceUnit;
	}

	/**
	 * @param priceUnit the priceUnit to set
	 */
	public void setPriceUnit(String priceUnit) {
		this.priceUnit = priceUnit;
	}

	/**
	 * @return the materialShortDesc
	 */
	public String getMaterialShortDesc() {
		return materialShortDesc;
	}

	/**
	 * @param materialShortDesc the materialShortDesc to set
	 */
	public void setMaterialShortDesc(String materialShortDesc) {
		this.materialShortDesc = materialShortDesc;
	}

	/**
	 * @return the supplierId
	 */
	public String getSupplierId() {
		return supplierId;
	}

	/**
	 * @param supplierId the supplierId to set
	 */
	public void setSupplierId(String supplierId) {
		this.supplierId = supplierId;
	}

	/**
	 * @return the catalogueType
	 */
	public String getCatalogueType() {
		return catalogueType;
	}

	/**
	 * @param catalogueType the catalogueType to set
	 */
	public void setCatalogueType(String catalogueType) {
		this.catalogueType = catalogueType;
	}

	/**
	 * @return the catalogueName
	 */
	public String getCatalogueName() {
		return catalogueName;
	}

	/**
	 * @param catalogueName the catalogueName to set
	 */
	public void setCatalogueName(String catalogueName) {
		this.catalogueName = catalogueName;
	}

	/**
	 * @return the priceAgreementStatus
	 */
	public String getPriceAgreementStatus() {
		return priceAgreementStatus;
	}

	/**
	 * @param priceAgreementStatus the priceAgreementStatus to set
	 */
	public void setPriceAgreementStatus(String priceAgreementStatus) {
		this.priceAgreementStatus = priceAgreementStatus;
	}

}
