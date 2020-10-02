package com.vf.ana;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import com.vf.ana.ent.DatePASDSummary;

@Repository
public class DatePASDAnaRepository {

	@Autowired
	MongoTemplate mongoTemplate;
	
	@Autowired
	PASDAnaRepository pASDAnaRepository;
	
	public void generateDatePASDNL() {
		
		mongoTemplate.dropCollection(DatePASDSummary.class);
		
		Map<String, Integer> str = pASDAnaRepository.getActiveOLAsByDateRange("2019-09-07", "2020-09-06", null);
		
		str.keySet().forEach(key->{
			DatePASDSummary dps = new DatePASDSummary();
			dps.setDate(LocalDate.parse(key, DateTimeFormatter.ISO_DATE));
			dps.setCount(str.get(key));
			dps.setDpasdId(UUID.randomUUID().toString());
			mongoTemplate.insert(dps);
		});
	}

	
}
