package com.vf.ana.ent;

import java.time.LocalDate;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.vf.ana.Constants;

@Document(collection = Constants.AGGREGATED_DATA_SECOND_LEVEL_COLLECTION_NAME)
public class AggregatedDateSecondLevel {

	@Id
	private String aggregatedDataSecondLevelId="";
	private String parentSupplierId="";
	private String tradingModel="";
	private String catalogueType="";
	private String opcoCode="";
	private String priceAgreementReferenceName="";
	private String materialGroupL4="";
	private String supplierPartNumber="";
	private LocalDate poDate = null;
	private LocalDate invoiceDate = null;
	private LocalDate validFromDate = null;
	private LocalDate validToDate = null;
	private double orderValue=0.0;;
	private double invoiceValue=0.0;;
	private double activePriceAgreements=0.0;;
	private double activeItems=0.0;;
	private double ordersIssued=0.0;;
	private double voucherValue=0.0;
	private double remVoucherValue=0.0;
	private double valueLeakageIdentified=0.0;
	private double valueLeakageRecovered=0.0;
	public String getAggregatedDataSecondLevelId() {
		return aggregatedDataSecondLevelId;
	}
	public void setAggregatedDataSecondLevelId(String aggregatedDataSecondLevelId) {
		this.aggregatedDataSecondLevelId = aggregatedDataSecondLevelId;
	}
	public String getParentSupplierId() {
		return parentSupplierId;
	}
	public void setParentSupplierId(String parentSupplierId) {
		this.parentSupplierId = parentSupplierId;
	}
	public String getTradingModel() {
		return tradingModel;
	}
	public void setTradingModel(String tradingModel) {
		this.tradingModel = tradingModel;
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
	public String getPriceAgreementReferenceName() {
		return priceAgreementReferenceName;
	}
	public void setPriceAgreementReferenceName(String priceAgreementReferenceName) {
		this.priceAgreementReferenceName = priceAgreementReferenceName;
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
	public LocalDate getPoDate() {
		return poDate;
	}
	public void setPoDate(LocalDate poDate) {
		this.poDate = poDate;
	}
	public LocalDate getInvoiceDate() {
		return invoiceDate;
	}
	public void setInvoiceDate(LocalDate invoiceDate) {
		this.invoiceDate = invoiceDate;
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
	public double getOrderValue() {
		return orderValue;
	}
	public void setOrderValue(double orderValue) {
		this.orderValue = orderValue;
	}
	public double getInvoiceValue() {
		return invoiceValue;
	}
	public void setInvoiceValue(double invoiceValue) {
		this.invoiceValue = invoiceValue;
	}
	public double getActivePriceAgreements() {
		return activePriceAgreements;
	}
	public void setActivePriceAgreements(double activePriceAgreements) {
		this.activePriceAgreements = activePriceAgreements;
	}
	public double getActiveItems() {
		return activeItems;
	}
	public void setActiveItems(double activeItems) {
		this.activeItems = activeItems;
	}
	public double getOrdersIssued() {
		return ordersIssued;
	}
	public void setOrdersIssued(double ordersIssued) {
		this.ordersIssued = ordersIssued;
	}
	public double getVoucherValue() {
		return voucherValue;
	}
	public void setVoucherValue(double voucherValue) {
		this.voucherValue = voucherValue;
	}
	public double getRemVoucherValue() {
		return remVoucherValue;
	}
	public void setRemVoucherValue(double remVoucherValue) {
		this.remVoucherValue = remVoucherValue;
	}
	public double getValueLeakageIdentified() {
		return valueLeakageIdentified;
	}
	public void setValueLeakageIdentified(double valueLeakageIdentified) {
		this.valueLeakageIdentified = valueLeakageIdentified;
	}
	public double getValueLeakageRecovered() {
		return valueLeakageRecovered;
	}
	public void setValueLeakageRecovered(double valueLeakageRecovered) {
		this.valueLeakageRecovered = valueLeakageRecovered;
	}
	
	
	
}
