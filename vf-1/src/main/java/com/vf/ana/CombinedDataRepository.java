package com.vf.ana;

import org.bson.BsonObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import com.vf.ana.ent.CombinedData;

public interface CombinedDataRepository extends MongoRepository<CombinedData, BsonObjectId> {

}
