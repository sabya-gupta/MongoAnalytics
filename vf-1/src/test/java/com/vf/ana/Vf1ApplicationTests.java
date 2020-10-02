package com.vf.ana;

import java.util.Arrays;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.vf.ana.ent.PriceAgreementSpnDetails;

@SpringBootTest
class Vf1ApplicationTests{

	@Autowired
	MongoTemplate mongoTemplate;

//	@Test
	void contextLoads() {
//		long num = mongoTemplate.getCollection("testDB").countDocuments();
//		System.out.println(num);

		try (MongoClient mongoClient = new MongoClient("localhost", 27017)) {

			MongoDatabase mongo = mongoClient.getDatabase("reporting");

			String grpBy = "{$group:{_id:{supplier: '$supp', country:'$cntry'}, totalOV: {$sum:'$ov'}}}";
			Bson grpByJson = BasicDBObject.parse(grpBy);

			String srtBy = "{$sort:{totalOV:-1}}";
			Bson srtByJson = BasicDBObject.parse(srtBy);
			AggregateIterable<Document> ret5 = mongo.getCollection("testDB")
					.aggregate(Arrays.asList(grpByJson, srtByJson));

			ret5.cursor().forEachRemaining(doc -> System.out
					.println("okok " + doc + " ok2 -> " + ((Document) doc.get("_id")).getInteger("Month")));

//			FindIterable<Document> ret = mongo.getCollection("testDB").find().filter(new Document("id", 1));
//			
//			ret.cursor().forEachRemaining(action->{
//				System.out.println("ov = "+action.get("ov"));
//			});
//			
//			
//			Document filter = new Document();
//			filter.append("supplier", "$supp");
//			filter.append("country", "$cntry");
//			
//									
//			Document multiplyDoc = new Document("$multiply", Arrays.asList("$ov", "$lk"));

//			AggregateIterable<Document> ret2 = mongo.getCollection("testDB").aggregate(Arrays.
//					asList(
//							group("$cntry", Accumulators.sum("TotalVal", multiplyDoc)),
//							sort(Sorts.descending("totalVal"))
//					)
//				)
//			;
//			
//			ret2.cursor().forEachRemaining(doc->System.out.println(doc));
//			
//			AggregateIterable<Document> ret3 = mongo.getCollection("testDB").aggregate(Arrays.
//					asList(
//							group(filter
//									, Accumulators.sum("TotalVal", "$ov")),
//							sort(Sorts.descending("totalVal"))
//					)
//				)
//			;
//			
//			ret3.cursor().forEachRemaining(doc->System.out.println(doc));

//			Document dtFilter = new Document();
//			dtFilter.append("Year", new Document("$year", "$dt"));
//			dtFilter.append("Month", new Document("$month", "$dt"));
//
//			Document DTFilter = new Document();
//			DTFilter.append("MonthYear", dtFilter);
//			
//			
//			Document ovd = new Document();
//			ovd.append("ov", "$ov");
//			
//			Document dtdM = new Document();
//			dtdM.append("Month", new Document("$month", "$dt"));
//			
//			Document dtdY = new Document();
//			dtdY.append("Year", new Document("$year", "$dt"));
//			
//			String[] ymFields = {"Month","Year"};
//
//			Document concMY = new Document("$concat", ymFields);
//			
//			
//			Document[] fields = {ovd, dtdY, dtdM};
//			Document[] fields2 = {ovd, concMY};
//			AggregateIterable<Document> ret4 = mongo.getCollection("testDB").aggregate(Arrays.
//					asList(
////							project(Projections.fields(fields))
////							,
//							group(dtFilter
//									, Accumulators.sum("TotalVal", "$ov"))
////							,
////							sort(Sorts.descending("Month"))
////							,
////							sort(Sorts.descending("Year"))
//					)
//				)
//			;
//			
//			ret4.cursor().forEachRemaining(doc->System.out.println("okok "+doc+" ok2 -> "
//			+
//					((Document)doc.get("_id")).getInteger("Month")
//			));
//			

		} catch (Exception ex) {
			ex.printStackTrace();
		}

//		Date date = getRandomDate();
//		System.out.println("-->> "+date.getDay()+"/"+date.getMonth()+"/"+date.getYear());
//		date = getRandomDate();
//		System.out.println("-->> "+date.getDay()+"/"+date.getMonth()+"/"+date.getYear());
//		date = getRandomDate();
//		System.out.println("-->> "+date.getDay()+"/"+date.getMonth()+"/"+date.getYear());
//		date = getRandomDate();
//		System.out.println("-->> "+date.getDay()+"/"+date.getMonth()+"/"+date.getYear());
//		date = getRandomDate();
//		System.out.println("-->> "+date.getDay()+"/"+date.getMonth()+"/"+date.getYear());
//		date = getRandomDate();
//		System.out.println("-->> "+date.getDay()+"/"+date.getMonth()+"/"+date.getYear());
//		date = getRandomDate();
//		System.out.println("-->> "+date.getDay()+"/"+date.getMonth()+"/"+date.getYear());
//

	}

//	@Test
//	public void updateTime() {
//		try( MongoClient mongoClient = new MongoClient( "localhost" , 27017 )){
//			
//			
//			MongoDatabase mongo = mongoClient.getDatabase("reporting");
//			FindIterable<Document> ret = mongo.getCollection("testDB").find();
//			
//			ret.cursor().forEachRemaining(obj->{
//				Date dt = getRandomDate();
//				obj.append("dt", dt);
//				System.out.println(((Date)obj.get("dt")).getYear());
//				mongo.getCollection("testDB").replaceOne(
//						Filters.eq("_id", obj.get("_id")), obj
//				);
//				System.out.println(obj.get("_id"));
//				
//			});
//			
//						
//			
//		}catch(Exception ex) {
//			ex.printStackTrace();
//		}
//		
//	}

//	@Value("${invoice.collName}")
//	private String invCollName;

	@Autowired
	InvGenerationUtil invGenerationUtil;

	@Autowired
	InvRepository invRepository;

//	@Test
	public void dummyTest() {
////		System.out.println("OK");
//		String spnNum = invGenerationUtil.getrandomSPNNumber();
//		String parentSupp = invGenerationUtil.getParentSupplier(spnNum);
//		double spnPrice = invGenerationUtil.getSPNUnitPrice(spnNum);
//		System.out.println(spnNum+"-"+parentSupp+" up = "+spnPrice);
//		
//		spnNum = invGenerationUtil.getrandomSPNNumber();
//		parentSupp = invGenerationUtil.getParentSupplier(spnNum);
//		spnPrice = invGenerationUtil.getSPNUnitPrice(spnNum);
//		System.out.println(spnNum+"-"+parentSupp+" up = "+spnPrice);
//
//		
//		spnNum = invGenerationUtil.getrandomSPNNumber();
//		parentSupp = invGenerationUtil.getParentSupplier(spnNum);
//		spnPrice = invGenerationUtil.getSPNUnitPrice(spnNum);
//		System.out.println(spnNum+"-"+parentSupp+" up = "+spnPrice);
//
//		
//		spnNum = invGenerationUtil.getrandomSPNNumber();
//		parentSupp = invGenerationUtil.getParentSupplier(spnNum);
//		spnPrice = invGenerationUtil.getSPNUnitPrice(spnNum);
//		System.out.println(spnNum+"-"+parentSupp+" up = "+spnPrice);
//
//		
//		spnNum = invGenerationUtil.getrandomSPNNumber();
//		parentSupp = invGenerationUtil.getParentSupplier(spnNum);
//		spnPrice = invGenerationUtil.getSPNUnitPrice(spnNum);
//		System.out.println(spnNum+"-"+parentSupp+" up = "+spnPrice);
//
//		
//		spnNum = invGenerationUtil.getrandomSPNNumber();
//		parentSupp = invGenerationUtil.getParentSupplier(spnNum);
//		spnPrice = invGenerationUtil.getSPNUnitPrice(spnNum);
//		System.out.println(spnNum+"-"+parentSupp+" up = "+spnPrice);
//
//		System.out.println(invGenerationUtil.getLocalmarket());
//		System.out.println(invGenerationUtil.getLocalmarket());
//		System.out.println(invGenerationUtil.getLocalmarket());
//		System.out.println(invGenerationUtil.getLocalmarket());
//		System.out.println(invGenerationUtil.getLocalmarket());
//		

//		AnaInv ana = invGenerationUtil.getRandomAnaInv();
		PriceAgreementSpnDetails pasd = invGenerationUtil.getRandomPASD();

//		invRepository.insertInv(ana);
		invRepository.insertPASD(pasd);

//		AnaInv ai = invRepository.getAnaInv(ana.getInvAnaId());
//
//		System.out.println("SPN entered =" + ana.getSPNNumber());
//		System.out.println("SPN in DB   =" + ai.getSPNNumber());
//
//		assertEquals(ana.getSPNNumber(), ana.getSPNNumber());
//
//		invRepository.getTotalLeakage();
	}

	@Test
	public void dummyTest2() {
		long t1 = System.currentTimeMillis();
//		LocalDate dt = invRepository.getEarliestDate();
//		System.out.println(dt);
//		
//		StringBuilder sb = new StringBuilder();
//		sb.append(dt.toString());
//		
//		while(LocalDate. .compareTo(LocalDate.now())<0) {
//			System.out.println("analysing "+dt);
//			dt.plusDays(1);
//		}

		
		
//		invRepository.generateDatePASD();
//		try {
////			for(int i=0; i<5; i++)
//			invRepository.generateDatePASD();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

//		
//		for(int i=0; i<500; i++) {
//			PriceAgreementSpnDetails pasd = invGenerationUtil.getRandomPASD();
//			invRepository.insertPASD(pasd);
//		}
		invRepository.getActiveOLAsByDate("2020-09-01", "2020-09-05");
		System.out.println("Time = "+((System.currentTimeMillis()-t1)));
	}

//	@Test
	public void dummyTest3() {

		for(int i=0; i<1000; i++) {
			PriceAgreementSpnDetails pasd = invGenerationUtil.getRandomPASD();			
			invRepository.insertPASD(pasd);
		}
	}

}
