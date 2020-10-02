

import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.excludeId;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Projections;

@Repository
public class PriceAgreementSpnDetailsRepository {
	@Autowired
	MongoTemplate mongoTemplate;
	
	Logger logger = LoggerFactory.getLogger(getClass());

   public void getAllLastOneYr() {
	   
	   String inputCollection = "priceAgreementSpnDetails";
	   String outputCollection = "DatewisePriceAgreements";
	   String databaseName = "vfDatabase";
	   String serverUrl = "localhost:27017";
	   int noOfPreviousDays = 3; // Getting Docs for these "no of days" only
	   
	   
	   
	   MongoClient mongoClient = new MongoClient(
			    new MongoClientURI(
			        "mongodb://" + serverUrl + "/?readPreference=primary&appname=MongoDB%20Compass&ssl=false"
			    )
			);
			
			LocalDate currentDate = LocalDate.now();
			LocalDate oneYrEarlierDate = currentDate.minus(noOfPreviousDays, ChronoUnit.DAYS); 
			int datesDiff = currentDate.compareTo(oneYrEarlierDate);
			
			System.out.println("currentDate::" + currentDate);
			System.out.println("oneYrEarlierDate::" + oneYrEarlierDate);
			System.out.println("date range difference:"+ datesDiff);
		
			MongoDatabase database = mongoClient.getDatabase(databaseName);
			MongoCollection<Document> collection = database.getCollection(inputCollection);
//			database.createCollection("DatewisePriceAgreements");
			MongoCollection<Document> outCollection = database.getCollection(outputCollection);
			outCollection.drop();
			
			LocalDate loopFromDate=null;
			LocalDate loopToDate = currentDate; 
			
			HashMap<String, ArrayList<String>> docMap = new HashMap<String, ArrayList<String>>();
			for (int cnt=1; cnt<= datesDiff; cnt++) {
				loopFromDate = loopToDate.minus(1, ChronoUnit.DAYS);
				System.out.print("loopFromDate::" + loopFromDate);
				System.out.println("loopToDate::" + loopToDate);
				
//				AggregateIterable<Document> newResult =
					collection.aggregate(Arrays.asList(match(or(Arrays.asList(or(Arrays.asList(and(gte("validFromDate", 
						loopFromDate), lte("validFromDate", 
								loopToDate)),eq("priceAgreementStatus", "Active"),  gte("validToDate",loopFromDate))), and(gt("validToDate",loopFromDate), lt("validToDate",
										loopToDate))))),
						project(Projections.fields(
								//Projections.excludeId(),
	                            Projections.include("supplierPartNumber", "opcoCode"), excludeId())),
						addFields(new Field("activeDate", loopFromDate)),
//						out("DatewisePriceAgreements")
						eq("$merge", eq("into", outputCollection))
	                            ))
						.toCollection();
			
				  loopToDate = loopFromDate; 
			}
	   
   }
	

	
	
}
