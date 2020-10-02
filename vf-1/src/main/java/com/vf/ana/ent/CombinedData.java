package com.vf.ana.ent;

import java.time.LocalDate;

import org.bson.BsonObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.vf.ana.Constants;

@Document(collection = Constants.COMBINED_SPN_DETAILS_COLLECTION_NAME)
public class CombinedData {
	@Id
	BsonObjectId _id;
	String tradingModel;
	String supplierId;
	String outlineAgreementNumber;
	String catalogueType;
	String opcoCode;
	String priceAgreementStatus;
	String parentSupplierId;
	LocalDate validFromDate;
	LocalDate validToDate;
	String materialGroupL4;
	String supplierPartNumber; 
	String priceAgreementReferenceName;
	LocalDate purchaseOrderCreationDate;
	LocalDate invoiceDate; 
	double ov;
	double iv;
	
	public BsonObjectId get_id() {
		return _id;
	}
	public void set_id(BsonObjectId _id) {
		this._id = _id;
	}
	public String getTradingModel() {
		return tradingModel;
	}
	public void setTradingModel(String tradingModel) {
		this.tradingModel = tradingModel;
	}
	public String getSupplierId() {
		return supplierId;
	}
	public void setSupplierId(String supplierId) {
		this.supplierId = supplierId;
	}
	public String getOutlineAgreementNumber() {
		return outlineAgreementNumber;
	}
	public void setOutlineAgreementNumber(String outlineAgreementNumber) {
		this.outlineAgreementNumber = outlineAgreementNumber;
	}
	public String getCatalogueType() {
		return catalogueType;
	}
	public void setCatalogueType(String catalogueType) {
		this.catalogueType = catalogueType;
	}
	public String getOpcoCode() {
		return opcoCode;
	}
	public void setOpcoCode(String opcoCode) {
		this.opcoCode = opcoCode;
	}
	public String getPriceAgreementStatus() {
		return priceAgreementStatus;
	}
	public void setPriceAgreementStatus(String priceAgreementStatus) {
		this.priceAgreementStatus = priceAgreementStatus;
	}
	public String getParentSupplierId() {
		return parentSupplierId;
	}
	public void setParentSupplierId(String parentSupplierId) {
		this.parentSupplierId = parentSupplierId;
	}
	public LocalDate getValidFromDate() {
		return validFromDate;
	}
	public void setValidFromDate(LocalDate validFromDate) {
		this.validFromDate = validFromDate;
	}
	public LocalDate getValidToDate() {
		return validToDate;
	}
	public void setValidToDate(LocalDate validToDate) {
		this.validToDate = validToDate;
	}
	public String getMaterialGroupL4() {
		return materialGroupL4;
	}
	public void setMaterialGroupL4(String materialGroupL4) {
		this.materialGroupL4 = materialGroupL4;
	}
	public String getSupplierPartNumber() {
		return supplierPartNumber;
	}
	public void setSupplierPartNumber(String supplierPartNumber) {
		this.supplierPartNumber = supplierPartNumber;
	}
	public String getPriceAgreementReferenceName() {
		return priceAgreementReferenceName;
	}
	public void setPriceAgreementReferenceName(String priceAgreementReferenceName) {
		this.priceAgreementReferenceName = priceAgreementReferenceName;
	}
	public LocalDate getPurchaseOrderCreationDate() {
		return purchaseOrderCreationDate;
	}
	public void setPurchaseOrderCreationDate(LocalDate purchaseOrderCreationDate) {
		this.purchaseOrderCreationDate = purchaseOrderCreationDate;
	}
	public LocalDate getInvoiceDate() {
		return invoiceDate;
	}
	public void setInvoiceDate(LocalDate invoiceDate) {
		this.invoiceDate = invoiceDate;
	}
	public double getOv() {
		return ov;
	}
	public void setOv(double ov) {
		this.ov = ov;
	}
	public double getIv() {
		return iv;
	}
	public void setIv(double iv) {
		this.iv = iv;
	} 
	
	
}
