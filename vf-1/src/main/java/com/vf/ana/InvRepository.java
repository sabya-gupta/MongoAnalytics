package com.vf.ana;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.swing.text.DateFormatter;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import com.vf.ana.ent.AnaInv;
import com.vf.ana.ent.DatePASD;
import com.vf.ana.ent.PriceAgreementSpnDetails;

@Repository
public class InvRepository {

	@Autowired
	MongoTemplate mongoTemplate;

	public void insertInv(AnaInv arg) {
		mongoTemplate.insert(arg);
	}

	public void insertPASD(PriceAgreementSpnDetails arg) {
		mongoTemplate.insert(arg);
	}

	public AnaInv getAnaInv(String id) {
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").is(id));
		return (AnaInv) mongoTemplate.findOne(query, AnaInv.class, "anaInv");
	}

	public void getPasdList(LocalDate date) {
		Query query = new Query();

		Criteria criteria = Criteria.where("priceAgreementStatus").is("Active")
				.orOperator(Criteria.where("validFromDate").gte(date), Criteria.where("validTomDate").gte(date));

		query.addCriteria(criteria);
		mongoTemplate.find(query, PriceAgreementSpnDetails.class, "priceAgreementSpnDetails").forEach(doc -> {
			System.out.println("from : " + doc.getValidFromDate() + " - to - " + doc.getValidToDate() + " active = "
					+ doc.getPriceAgreementStatus());
		});
	}

	public void generateDatePASDNew() throws InterruptedException {

		mongoTemplate.dropCollection(DatePASD.class);

		LocalDate date = LocalDate.now().minusDays(365);

		System.out.println("EARLIEST DATE IS NEW : " + date);

		final List<LocalDate> dateList = new ArrayList<LocalDate>();

		dateList.add(date);

		int numThreads = 2;

//		
		ArrayList<ArrayList<LocalDate>> datePools = new ArrayList<ArrayList<LocalDate>>();

		for (int i = 0; i < numThreads; i++) {
			ArrayList<LocalDate> dtList = new ArrayList<LocalDate>();
			datePools.add(dtList);
		}

		int cntr = 0;
		while (date.compareTo(LocalDate.now()) < 1) {
			// System.out.println("adding in "+(cntr%5)+" - "+date);
			datePools.get(cntr % (numThreads)).add(date);
			date = date.plusDays(1);
			cntr++;
		}

		for (int i = 0; i < numThreads; i++) {
			ArrayList<LocalDate> dtList = datePools.get(i);
//			System.out.println(i+" +++==size = "+dtList.size());
			PasdThread pt = new PasdThread(dtList, mongoTemplate, "Thread-" + i, pasdRepository);
			pt.start();
			pt.join();
		}

	}

	@Autowired
	PasdRepository pasdRepository;

	public void generateDatePASD() {
		mongoTemplate.dropCollection(DatePASD.class);
		final LocalDate date = LocalDate.now().minusDays(365);

		System.out.println("EARLIEST DATE IS : " + date);

		final List<LocalDate> dateList = new ArrayList<LocalDate>();

		dateList.add(date);
		while (dateList.get(0).compareTo(LocalDate.now()) < 1) {

			Query query = new Query();

			Criteria criteria = Criteria.where("priceAgreementStatus").is("Active").andOperator(
					Criteria.where("validFromDate").lte(dateList.get(0)),
					Criteria.where("validToDate").gte(dateList.get(0)));

			query.addCriteria(criteria);

			mongoTemplate.find(query, PriceAgreementSpnDetails.class, "priceAgreementSpnDetails").forEach(doc -> {
				DatePASD dp = new DatePASD();
				dp.setDate(dateList.get(0));
				dp.setDpasdId(UUID.randomUUID().toString());
				dp.setPasd(doc);
				mongoTemplate.insert(dp);
			});
			LocalDate dte = dateList.get(0).plusDays(1);
			dateList.remove(0);
			dateList.add(dte);
		}

	}

	public void generateDatePASDNL() {
		mongoTemplate.dropCollection(DatePASD.class);
		final LocalDate date = LocalDate.now().minusDays(365);

		System.out.println("EARLIEST DATE IS : " + date);

		final List<LocalDate> dateList = new ArrayList<LocalDate>();

		dateList.add(date);

		Query query = new Query();

		Criteria criteria = Criteria.where("priceAgreementStatus").is("Active").orOperator(
				Criteria.where("validFromDate").lte(dateList.get(0)),
				Criteria.where("validToDate").gte(LocalDate.now()));

		query.addCriteria(criteria);

		mongoTemplate.find(query, PriceAgreementSpnDetails.class, "priceAgreementSpnDetails").forEach(pasd -> {
			LocalDate currDate = dateList.get(0);
			while (currDate.compareTo(LocalDate.now()) < 1) {


				if (pasd.getValidFromDate().compareTo(currDate) <= 0
						&& pasd.getValidToDate().compareTo(currDate) >= 0) {

					DatePASD dp = new DatePASD();
					dp.setDate(dateList.get(0));
					dp.setDpasdId(UUID.randomUUID().toString());
					dp.setPasd(pasd);

					mongoTemplate.insert(dp);
				}

				currDate = currDate.plusDays(1);
			}

		});

	}

//	public LocalDate getEarliestDate() {
//		
//		
//		LocalDate startDate = null;
//		
//		
//		String qry1 = "{$match:{priceAgreementStatus:'Active'}}";
//		String qry2 = "{$group:{_id: null, mindate:{'$min':'$validFromDate'}}}";
//		
//		
//		Bson q1Qry1 = BasicDBObject.parse(qry1);
//		Bson q1Qry2 = BasicDBObject.parse(qry2);
//
//		MongoDatabase mongo = mongoTemplate.getDb();
//		AggregateIterable<Document> ret5 = mongo.getCollection("priceAgreementSpnDetails").aggregate(Arrays.
//				asList(
//						q1Qry1, q1Qry2
//				)
//			)
//		;
//		
//		
//		List<LocalDate> ldList = new ArrayList<>();
//		
//		ret5.cursor().forEachRemaining(doc->{
//			Date dt123 = doc.getDate("mindate");
//			LocalDate ld = dt123.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
//			ldList.add(ld);
//		});
//		
//		return ldList.get(0);
//	}

	public void getTotalLeakage() {

		MongoDatabase mongo = mongoTemplate.getDb();

		String grpBy = "{$group:{_id:null, totalLeakage:{$sum:'$leakage'}}}";
		Bson grpByJson = BasicDBObject.parse(grpBy);

		AggregateIterable<Document> ret5 = mongo.getCollection("anaInv").aggregate(Arrays.asList(grpByJson));

		ret5.cursor().forEachRemaining(doc -> {
			System.out.println("Total Leakage is : ->" + doc.getDouble("totalLeakage"));
		});

	}

	public long getNumberOfActiveSPN() {

		MongoDatabase mongo = mongoTemplate.getDb();

		String dateField = "validToDate";

		LocalDate dt = LocalDate.now();
		String strDate = dt.format(DateTimeFormatter.ISO_DATE);

		String strFilter = "{'" + dateField + "': {$gte:ISODate('" + strDate + "')}}";
		Bson dateFilter = BasicDBObject.parse(strFilter);

		Filters.gte(dateField, "ISODate('" + strDate + "')");

		return mongo.getCollection("priceAgreementSpnDetails").countDocuments(dateFilter);
	}

	public int getNumberOfActiveOLA() {

		String dateField = "validToDate";
		String olaField = "outlineAgreementNumber";

		// Project
		String strProjectQuery = "{$project:{" + dateField + ":'$" + dateField + "', " + olaField + ":'$" + olaField
				+ "'}}";
		Bson projectQry = BasicDBObject.parse(strProjectQuery);

		// Match
		LocalDate dt = LocalDate.now();
		String strDate = dt.format(DateTimeFormatter.ISO_DATE);
		String strMatchQuery = "{$match:{'" + dateField + "': {$gte:ISODate('" + strDate + "')}}}";
		Bson matchQry = BasicDBObject.parse(strMatchQuery);

		// Group By
		String strGrpQry = "{$group:{_id:'$" + olaField + "', cnt:{$sum:1}}}";
		Bson grpQry = BasicDBObject.parse(strGrpQry);

		// Sum agg
		String strSumQry = "{$group:{_id: null, numGettingExpired:{$sum:1}}}";
		Bson sumQry = BasicDBObject.parse(strSumQry);

		MongoDatabase mongo = mongoTemplate.getDb();

		AggregateIterable<Document> ret5 = mongo.getCollection("priceAgreementSpnDetails")
				.aggregate(Arrays.asList(projectQry, matchQry, grpQry, sumQry));
		List<Document> docList = new ArrayList<Document>();
		ret5.cursor().forEachRemaining(doc -> {
			docList.add(doc);
		});

		return docList.get(0).getInteger("numGettingExpired", -1);
	}

	public void getPriceAgreementBydate() {
		String q1 = "{$project:{validToDate:'$validToDate', supplierPartNumber:'$supplierPartNumber', opcoCode:'$opcoCode', priceAgreementStatus:'$priceAgreementStatus'}}";
		Bson q1Qry = BasicDBObject.parse(q1);

		String q2 = "{$match: { 'priceAgreementStatus':{$eq:'Inactive'}, 'validToDate': {$gte:ISODate('2020-09-02'), $lte:ISODate('2020-09-05')}}}";
		Bson q2Qry = BasicDBObject.parse(q2);

		String q3 = "{$group:{_id:{validToDate:'$validToDate', supplierPartNumber:'$supplierPartNumber', opcoCode:'$opcoCode', priceAgreementStatus:'$priceAgreementStatus'}, count:{'$sum' : 1}}}";
		Bson q3Qry = BasicDBObject.parse(q3);

		String q4 = "{$project:{validToDate:'$_id.validToDate'}}";
		Bson q4Qry = BasicDBObject.parse(q4);

		String q5 = "{$group:{_id:'$validToDate', count:{'$sum' : 1}}}";
		Bson q5Qry = BasicDBObject.parse(q5);

		String q6 = "{$sort:{_id.validToDate:1}}";
		Bson q6Qry = BasicDBObject.parse(q5);

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection("priceAgreementSpnDetails")
				.aggregate(Arrays.asList(q1Qry, q2Qry, q3Qry, q4Qry, q5Qry));

		TreeMap<Date, Integer> tm = new TreeMap<Date, Integer>();
		ret5.cursor().forEachRemaining(doc -> {
			System.out.println(doc.getDate("_id") + " - " + doc.getInteger("count"));
			tm.put(doc.getDate("_id"), doc.getInteger("count"));
			System.out.println(doc);
			;
		});

		System.out.println(tm);
	}

	public Map<Date, Integer> getActiveOLAsByDateOld(String date1, String date2) {

		String q1 = "{$match:{'date': {$gte:ISODate('" + date1 + "'), $lt:ISODate('" + date2 + "')}}}";
		Bson q1Qry = BasicDBObject.parse(q1);

		String q2 = "{ $group : {_id: {date:'$date', ola:'$pasd.outlineAgreementNumber'}, count:{'$sum':1}}}";
		Bson q2Qry = BasicDBObject.parse(q2);

		String q3 = "{$project:{thedate:'$_id.date'}}";
		Bson q3Qry = BasicDBObject.parse(q3);

		String q4 = "{$group: {_id:'$thedate', count:{'$sum':1}}}";
		Bson q4Qry = BasicDBObject.parse(q4);

		String q5 = "{$sort:{_id:1}}";
		Bson q5Qry = BasicDBObject.parse(q5);

//		String q6 = "{$sort:{_id:-1}}";
//		Bson q6Qry = BasicDBObject.parse(q5);

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection("dpasd")
				.aggregate(Arrays.asList(q1Qry, q2Qry, q3Qry, q4Qry, q5Qry));

		Map<Date, Integer> tm = new TreeMap<Date, Integer>();
		ret5.cursor().forEachRemaining(doc -> {
			System.out.println(doc);
			;
			tm.put(doc.getDate("_id"), doc.getInteger("count"));
		});

		System.out.println(tm);

		return tm;

	}

	public Map<Date, Integer> getActiveOLAsByDate(String date1, String date2) {

		Bson q1Qry = match(
				Filters.and(Filters.gte("date", LocalDate.parse(date1, DateTimeFormatter.ISO_DATE)), Filters.lte("date", LocalDate.parse(date2, DateTimeFormatter.ISO_DATE))));

		Map<String, Object> multiIdMap1 = new HashMap<String, Object>();
		multiIdMap1.put("date", "$date");
		multiIdMap1.put("ola", "$pasd.outlineAgreementNumber");

		Document groupFields1 = new Document(multiIdMap1);
		Bson q2Qry = group(groupFields1, Accumulators.sum("count", 1));

		Map<String, Object> multiIdMap2 = new HashMap<String, Object>();
		multiIdMap2.put("thedate", "$_id.date");
		Document groupFields2 = new Document(multiIdMap2);
		Bson q3Qry = project(Projections.fields(groupFields2));

		Bson q4Qry = group("$thedate", Accumulators.sum("count", 1));

		Bson q5Qry = sort(Sorts.ascending("_id"));

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret5 = mongo.getCollection("dpasd")
				.aggregate(Arrays.asList(q1Qry, q2Qry, q3Qry, q4Qry, q5Qry));

		Map<Date, Integer> tm = new TreeMap<Date, Integer>();
		ret5.cursor().forEachRemaining(doc -> {
			System.out.println(doc);
			;
			tm.put(doc.getDate("_id"), doc.getInteger("count"));
		});

		System.out.println(tm);

		return tm;

	}

}

class PasdThread extends Thread {

	List<LocalDate> dateList;
	PasdRepository pasdRepository;
	String name;
	MongoTemplate mongoTemplate;

	public PasdThread(List<LocalDate> dateList, MongoTemplate mongoTemplate, String name,
			PasdRepository pasdRepository) {
		super();

		this.dateList = dateList;
		this.pasdRepository = pasdRepository;
		this.name = name;
		this.mongoTemplate = mongoTemplate;
	}

	public void run() {

		while (dateList.size() > 0) {

//			System.out.println(name+"->  DateSize = "+dateList.size());
//			System.out.println(name+"->  Date = "+dateList);

			Query query = new Query();

			List<DatePASD> collect = new ArrayList<DatePASD>();

			Criteria criteria = Criteria.where("priceAgreementStatus").is("Active").andOperator(
					Criteria.where("validFromDate").lte(dateList.get(0)),
					Criteria.where("validToDate").gte(dateList.get(0)));

			query.addCriteria(criteria);

			mongoTemplate.find(query, PriceAgreementSpnDetails.class, "priceAgreementSpnDetails").forEach(doc -> {
//				System.out.println("name "+name+" -> "+doc);
				DatePASD dp = new DatePASD();
				dp.setDate(dateList.get(0));
				dp.setDpasdId(UUID.randomUUID().toString());
				dp.setPasd(doc);
				collect.add(dp);
				// mongoTemplate.insert(dp);
			});

//			pasdRepository.saveAll(collect);
//			System.out.println(name+"->  DateSize3 = "+dateList.size());
			dateList.remove(0);
//			System.out.println(name+"->  DateSize4 = "+dateList.size());
		}

	}
}