package com.vf.ana.ent;

import java.util.Date;
import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "anaInv")
public class AnaInv {

	@Id
	private String invAnaId="";
	private String invNumber="";
	private String olaNumber="";
	private String SPNNumber="";
	private String PONumber="";
	private String parentSupplier="";
	private String childSupplier="";
	private String L4MatCat="";
	private String L1MatCat="";
	private String tradingModel="";
	private String companyCode="";
	private int invQty=0;
	private int orderQty=0;
	private Date invDate=null;
	private Date poDate=null;
	private String purOrg="";
	private String matNum="";
	private String poCurr="";
	private String invCurr="";
	private double invRateInBaseCurr=0;
	private double poRateInBaseCurr=0;
	private double leakage=0;
	private int priceUnit=1;
	private double netPrice=0;
	private Map<String, String> localMarket;
	public String getInvAnaId() {
		return invAnaId;
	}
	public void setInvAnaId(String invAnaId) {
		this.invAnaId = invAnaId;
	}
	public String getSPNNumber() {
		return SPNNumber;
	}
	public void setSPNNumber(String sPNNumber) {
		SPNNumber = sPNNumber;
	}
	public String getPONumber() {
		return PONumber;
	}
	public void setPONumber(String pONumber) {
		PONumber = pONumber;
	}
	public String getL4MatCat() {
		return L4MatCat;
	}
	public void setL4MatCat(String l4MatCat) {
		L4MatCat = l4MatCat;
	}
	public String getL1MatCat() {
		return L1MatCat;
	}
	public void setL1MatCat(String l1MatCat) {
		L1MatCat = l1MatCat;
	}
	public int getInvQty() {
		return invQty;
	}
	public void setInvQty(int invQty) {
		this.invQty = invQty;
	}
	public int getOrderQty() {
		return orderQty;
	}
	public void setOrderQty(int orderQty) {
		this.orderQty = orderQty;
	}
	public String getInvNumber() {
		return invNumber;
	}
	public void setInvNumber(String invNumber) {
		this.invNumber = invNumber;
	}
	public Date getInvDate() {
		return invDate;
	}
	public void setInvDate(Date invDate) {
		this.invDate = invDate;
	}
	public Date getPoDate() {
		return poDate;
	}
	public void setPoDate(Date poDate) {
		this.poDate = poDate;
	}
	public String getPurOrg() {
		return purOrg;
	}
	public void setPurOrg(String purOrg) {
		this.purOrg = purOrg;
	}
	public String getMatNum() {
		return matNum;
	}
	public void setMatNum(String matNum) {
		this.matNum = matNum;
	}
	public String getPoCurr() {
		return poCurr;
	}
	public void setPoCurr(String poCurr) {
		this.poCurr = poCurr;
	}
	public String getInvCurr() {
		return invCurr;
	}
	public void setInvCurr(String invCurr) {
		this.invCurr = invCurr;
	}
	public double getInvRateInBaseCurr() {
		return invRateInBaseCurr;
	}
	public void setInvRateInBaseCurr(double invRateInBaseCurr) {
		this.invRateInBaseCurr = invRateInBaseCurr;
	}
	public double getPoRateInBaseCurr() {
		return poRateInBaseCurr;
	}
	public void setPoRateInBaseCurr(double poRateInBaseCurr) {
		this.poRateInBaseCurr = poRateInBaseCurr;
	}
	public int getPriceUnit() {
		return priceUnit;
	}
	public void setPriceUnit(int priceUnit) {
		this.priceUnit = priceUnit;
	}
	public double getNetPrice() {
		return netPrice;
	}
	public void setNetPrice(double netPrice) {
		this.netPrice = netPrice;
	}
	public double getLeakage() {
		return leakage;
	}
	public void setLeakage(double leakage) {
		this.leakage = leakage;
	}
	public Map<String, String> getLocalMarket() {
		return localMarket;
	}
	public void setLocalMarket(Map<String, String> localMarket) {
		this.localMarket = localMarket;
	}
	public String getOlaNumber() {
		return olaNumber;
	}
	public void setOlaNumber(String olaNumber) {
		this.olaNumber = olaNumber;
	}
	public String getParentSupplier() {
		return parentSupplier;
	}
	public void setParentSupplier(String parentSupplier) {
		this.parentSupplier = parentSupplier;
	}
	public String getChildSupplier() {
		return childSupplier;
	}
	public void setChildSupplier(String childSupplier) {
		this.childSupplier = childSupplier;
	}
	public String getTradingModel() {
		return tradingModel;
	}
	public void setTradingModel(String tradingModel) {
		this.tradingModel = tradingModel;
	}
	public String getCompanyCode() {
		return companyCode;
	}
	public void setCompanyCode(String companyCode) {
		this.companyCode = companyCode;
	}
	
	
	
	
}
