package com.vf.ana;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.vf.ana.ent.DatePASD;
import com.vf.ana.ent.PriceAgreementSpnDetails;

public interface PasdRepository extends MongoRepository<PriceAgreementSpnDetails, String>{

}
