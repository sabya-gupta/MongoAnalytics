package com.vf.ana.ent;

import java.time.LocalDate;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "dpasdsummary")
public class DatePASDSummary {

	
	@Id
	private String dpasdId;

	public String getDpasdId() {
		return dpasdId;
	}
	public void setDpasdId(String dpasdId) {
		this.dpasdId = dpasdId;
	}
	public LocalDate getDate() {
		return date;
	}
	public void setDate(LocalDate date) {
		this.date = date;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	LocalDate date;
	int count;

	
}
