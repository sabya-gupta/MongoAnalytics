package com.vf.ana.ent;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.vf.ana.Constants;

@Document(collection = Constants.AGGREGATED_DATA_FIRST_LEVEL_COLLECTION_NAME)
public class AggregatedDataFirstLevel {

	
	@Id
	private String aggregatedDataFirstLevelId="";
	private String propName="";
	private String propVal="";
	private double orderValue=0.0;;
	private double invoiceValue=0.0;;
	private double activePriceAgreements=0.0;;
	private double activeItems=0.0;;
	private double ordersIssued=0.0;;
	private double voucherValue=0.0;
	private double remVoucherValue=0.0;
	private double valueLeakageIdentified=0.0;
	private double valueLeakageRecovered=0.0;

	
	
	@Override
	public String toString() {
		return "AggregatedDataFirstLevel [aggregatedDataFirstLevelId=" + aggregatedDataFirstLevelId + ", propName="
				+ propName + ", propVal=" + propVal + ", orderValue=" + orderValue + ", invoiceValue=" + invoiceValue
				+ ", activePriceAgreements=" + activePriceAgreements + ", activeItems=" + activeItems
				+ ", ordersIssued=" + ordersIssued + ", voucherValue=" + voucherValue + ", remVoucherValue="
				+ remVoucherValue + ", valueLeakageIdentified=" + valueLeakageIdentified + ", valueLeakageRecovered="
				+ valueLeakageRecovered + "]";
	}
	public String getAggregatedDataFirstLevelId() {
		return aggregatedDataFirstLevelId;
	}
	public void setAggregatedDataFirstLevelId(String aggregatedDataFirstLevelId) {
		this.aggregatedDataFirstLevelId = aggregatedDataFirstLevelId;
	}
	public String getPropName() {
		return propName;
	}
	public void setPropName(String propName) {
		this.propName = propName;
	}
	public String getPropVal() {
		return propVal;
	}
	public void setPropVal(String propVal) {
		if(propVal==null || propVal.trim().length()==0) propVal="NA";
		this.propVal = propVal;
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
