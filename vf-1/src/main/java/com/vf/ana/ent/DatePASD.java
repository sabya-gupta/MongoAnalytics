package com.vf.ana.ent;

import java.time.LocalDate;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;


@Document(collection = "dpasd")
public class DatePASD {
	
	public String getDpasdId() {
		return dpasdId;
	}
	public void setDpasdId(String dpasdId) {
		this.dpasdId = dpasdId;
	}
	@Id
	private String dpasdId;

	LocalDate date;
	PriceAgreementSpnDetails pasd;
	public LocalDate getDate() {
		return date;
	}
	public void setDate(LocalDate date) {
		this.date = date;
	}
	public PriceAgreementSpnDetails getPasd() {
		return pasd;
	}
	public void setPasd(PriceAgreementSpnDetails pasd) {
		this.pasd = pasd;
	}
	

}
