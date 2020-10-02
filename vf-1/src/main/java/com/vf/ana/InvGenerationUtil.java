package com.vf.ana;

import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.stereotype.Service;

import com.vf.ana.ent.AnaInv;
import com.vf.ana.ent.PriceAgreementSpnDetails;

@Service
public class InvGenerationUtil {


	public String getrandomSPNNumber() {
		return "SPN"+ new Random().nextInt(100);
	}
	
	public String getrandomPONumber() {
		return "PO"+ new Random().nextInt(10000);
	}
	
	public String getrandomInvNumber() {
		return "INV"+ new Random().nextInt(55555);
	}
	
	
	public Date getRandomDate() {
		@SuppressWarnings("deprecation")
		Date startInclusive = new Date(117, 1, 1);
		@SuppressWarnings("deprecation")
		Date endExclusive = new Date(120, 8, 15);
	    long startMillis = startInclusive.getTime();
	    long endMillis = endExclusive.getTime();
	    long randomMillisSinceEpoch = ThreadLocalRandom
	      .current()
	      .nextLong(startMillis, endMillis);
	 
	    return new Date(randomMillisSinceEpoch);
	}
	
	
	public String getParentSupplier(String sPNNumber) {
		String parentSuppliers[] = {"HUWAWEI", "LENOVO", "SEDUS", "NOKIA", "SAMSUNG"};
		int spnNum = Integer.parseInt(sPNNumber.substring(3));
		return parentSuppliers[spnNum%5];
	}
	
	public Map<String, String> getLocalmarket() {
		String localmarketCodes[] = {"United Kingdom", "Romania", "Turkey"};
		String lm = localmarketCodes[new Random().nextInt(3)];
		Map<String, String> lmHm = new HashMap<>();
		lmHm.put("code", lm);
		lmHm.put("desc", lm);
		return lmHm;
	}
	
	public double getSPNUnitPrice(String sPNNumber) {
		int spnNum = Integer.parseInt(sPNNumber.substring(3));
		return spnNum/10.00;
	}
	
	public String getL1cat(String sPNNumber) {
		String L1cats[] = {"NETWORK", "ITSERVICES", "HARDWARE", "HANDSET", "SIMCARDS"};
		int spnNum = Integer.parseInt(sPNNumber.substring(3));
		return L1cats[spnNum%5];
	}
	
	public PriceAgreementSpnDetails getRandomPASD() {
		PriceAgreementSpnDetails pasd = new PriceAgreementSpnDetails();
		
		int ran = new Random().nextInt(50);

		String dtStr = "2008"+"-"+"09"+"-"+"01";
		String dtStr2 = "2021"+"-"+"09"+"-"+"05";

		pasd.setPriceAgreementStatus("Active");
		pasd.setOpcoCode("OPO-"+(new Random().nextInt(10)));
		pasd.setSupplierPartNumber("SPN-"+(new Random().nextInt(10)));
		pasd.setCatalogueName("CAT"+ ran);
		pasd.setCatalogueType("CATTYPE"+ran);
		pasd.setMaterialGroupL4("L4"+ran);
		pasd.setMaterialShortDesc("MATSD"+ran);
		pasd.setValidFromDate(LocalDate.parse(dtStr));
		pasd.setValidToDate(LocalDate.parse(dtStr2));
		pasd.setOutlineAgreementNumber("OLA-"+(new Random().nextInt(5)));
		return pasd;
	}
	
	public AnaInv getRandomAnaInv() {
		AnaInv anaInv = new AnaInv();
		String spnNum = getrandomSPNNumber();
		String parentSupp = getParentSupplier(spnNum);

		anaInv.setInvNumber(getrandomInvNumber());
		anaInv.setPONumber(getrandomPONumber());
		anaInv.setSPNNumber(spnNum);
		String L1matCode = getL1cat(spnNum);
		anaInv.setL1MatCat(L1matCode);
		Date dt = getRandomDate();
		anaInv.setInvDate(dt);
		anaInv.setPoDate(dt);
		Map<String, String> lm = getLocalmarket();
		anaInv.setLocalMarket(lm);
		anaInv.setOlaNumber(spnNum+"_"+lm.get("code")+"_"+L1matCode);
		anaInv.setParentSupplier(parentSupp);
		
		int invQty = new Random().nextInt(1000);
		
		anaInv.setInvQty(invQty);
		anaInv.setOrderQty(new Random().nextInt(1000));
		
		double invUnitPrice = getSPNUnitPrice(spnNum);
		
		anaInv.setInvRateInBaseCurr(invUnitPrice);
		anaInv.setPoRateInBaseCurr(getSPNUnitPrice(spnNum));
		
		double leakage = (invQty*invUnitPrice*(new Random().nextInt(10)))/100;
		
		anaInv.setLeakage(leakage);
		
		anaInv.setInvAnaId(UUID.randomUUID().toString());
		return anaInv;
	}
	

}
