package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.regex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;

@Repository
public class TopLevelAnalyticsUtilLeakageValue {

	@Autowired
	Common common;
	
	@Autowired
	MongoTemplate mongoTemplate;

	Logger logger = LoggerFactory.getLogger(getClass());

	public double getTotalLeakageValue(Map<String, List<String>> argfilter, List<String> yyyymm) {

//		String collectionName = "leakage";
		
		String collectionName = Constants.VALUE_LEAKAGE_COLLECTION_NAME;
		

		List<Bson> pipeline = new ArrayList<>();

		Map<String, List<String>> filter = null;

		if (argfilter != null) {
			filter = new HashMap<String, List<String>>();
			filter.putAll(argfilter);
		}

//		logger.debug("{} - {}", groupByPropName, orderByKPIField);

		if (filter != null) {

			Bson bfilterClause = common.formMatchClauseForListFilterBson(filter);
			if (bfilterClause != null)
				pipeline.add(bfilterClause);
		}



		Bson firstProjectVLBson = BasicDBObject
				.parse("" + "{$project: {" 
						+ "vl: {$slice:['$valueLeakages', 1]}}" + "}");
		pipeline.add(firstProjectVLBson);

//		printDocs(collectionName, pipeline);

		pipeline.add(BasicDBObject.parse("{$unwind: { path: '$vl', preserveNullAndEmptyArrays: true } }"));

//		printDocs(collectionName, pipeline);

		Bson secondProjectVLBson = BasicDBObject.parse("{$project : {" 
				+ "dtt:'$vl.calculationDate', "
				+ "yyyymm: {$dateToString: { format: '%Y-%m', date: '$vl.calculationDate' }}, "
				+ "val: '$vl.leakageValue' " + "} }");
		pipeline.add(secondProjectVLBson);

//		printDocs(collectionName, pipeline);

		// add dateFilters
		if (yyyymm != null && yyyymm.size() > 0) {
			Bson dateFilter = match(in("yyyymm", yyyymm));
			pipeline.add(dateFilter);
		}


		String grpByLV = "{$group:{_id: null ,  val:{$sum:'$val'} " + "}}";

		pipeline.add(BasicDBObject.parse(grpByLV));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collectionName).aggregate(pipeline);

		printDocs(collectionName, pipeline);
		
		double val = 0;
		
		
		try {
			val = ret.first().getDouble("val");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return val;
	}
	
	private void printDocs(String collection, List<Bson> pipeline) {
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug(">>>>>>>>>>{}", doc);
		});

	}

}
