package com.vf.ana;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoDatabase;

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
import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.in;

@Component
public class Common {

	Logger logger = LoggerFactory.getLogger(getClass());

	public String formMatchClauseForFilter2(Map<String, String> filterMap, String dateFilterName, String date) {

		if (filterMap == null)
			return null;

		final StringBuilder expr = new StringBuilder();

		final StringBuilder comma = new StringBuilder();

		filterMap.keySet().forEach(key -> {
			String filter = "{" + key + " : '" + filterMap.get(key) + "'}";
			logger.debug("filter = {}", filter);
			expr.append(comma).append(filter);
			if (comma.length() == 0)
				comma.append(" , ");
		});

		logger.debug(">>>>expr = {}", expr);

		if (date != null) {
			String startDate = date;
			LocalDate dt = LocalDate.parse(date, DateTimeFormatter.ISO_DATE);
			dt = dt.plusDays(1);
			String endDate = dt.format(DateTimeFormatter.ISO_DATE);
			String filter = "{" + dateFilterName + ":{$gte:ISODate('" + startDate + "')}}";
			expr.append(comma).append(filter);
			logger.debug("filter = {} expr = {}", filter, expr);

			String filter2 = "{" + dateFilterName + ":{$lte:ISODate('" + endDate + "')}}";
			logger.debug("filter = {} expr = {}", filter2, expr);
			expr.append(comma).append(filter2);
		}
		// {validFromDate:{$lte:ISODate('2020-09-06')}}
		logger.debug("expr = {}", expr);
		String qMatch = "{$match:{$and:[" + expr.toString() + "]}}";

		return qMatch;
	}

	public String formMatchClauseForListFilter(Map<String, List<String>> filterMap, String type, List<String> dates) {

		if (filterMap == null)
			return null;

		final StringBuilder expr = new StringBuilder();

		final StringBuilder comma = new StringBuilder();

		filterMap.keySet().forEach(key -> {
			String filter = "{" + key + " : { $in: " + filterMap.get(key).toString() + "}}";
			logger.debug("filter = {}", filter);
			expr.append(comma).append(filter);
			if (comma.length() == 0)
				comma.append(" , ");
		});

		logger.debug("expr = {}", expr);
		String qMatch = "{$match:{$and:[" + expr.toString() + "]}}";
		logger.debug(">>>>expr = {}", qMatch);

		return qMatch;
	}

	public Bson formMatchClauseForListFilterBson(Map<String, List<String>> filterMap) {

		if (filterMap == null)
			return null;

		List<Bson> andFilters = new ArrayList<>();

		for (String key : filterMap.keySet()) {
			Bson filter = in(key, filterMap.get(key));
			andFilters.add(filter);
		}

		return match(and(andFilters));

	}

	public Bson getYYYYMMFilterForPAandAI(List<String> yyyymm) {
		List<Bson> dateFilterList = new ArrayList<>();
		for (String ym : yyyymm) {
			LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
			Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
			dateFilterList.add(df);
		}
		Bson orDf = match(or(dateFilterList));
		return orDf;
	}
	
	
	
	public int getCount(String collection, List<Bson> pipeline, MongoTemplate mongoTemplate) {

		List<Bson> newpipeline = new ArrayList<Bson>();
		newpipeline.addAll(pipeline);

		String cntStr = "{$group:{_id:null, val:{'$sum':1}}}";
		newpipeline.add(BasicDBObject.parse(cntStr));

		MongoDatabase mongo = mongoTemplate.getDb();

		int count = 0;

		try {
//			printDocs(collection, pipeline);
//			printDocs(collection, newpipeline);
			count = mongo.getCollection(collection).aggregate(newpipeline).first().getInteger("val");
		} catch (NullPointerException e) {

		} catch (Exception e) {
			e.printStackTrace();
		}

		return count;
	}

	public void printDocs(String collection, List<Bson> pipeline, MongoTemplate mongoTemplate) {
		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug(">>>>>>>>>>{}", doc);
		});

	}

	public void getResults(List<Bson> pipeline, int dir, int pgNum, final Map<String, Map<String, Double>> retMap,
			List<String> kpis, String collection, String sortByField, MongoTemplate mongoTemplate) {

		if (sortByField != null && dir == Constants.SORT_DIRECTION_ASCENDING && retMap.size() == 0) {
			String ordStr = "{$sort:{" + sortByField + ":1}}";
			Bson bord = BasicDBObject.parse(ordStr);
			pipeline.add(bord);
		}
//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		if (sortByField != null && dir == Constants.SORT_DIRECTION_DESCENDING && retMap.size() == 0) {
			String ordStr = "{$sort:{" + sortByField + ":-1}}";
			Bson bord = BasicDBObject.parse(ordStr);
			pipeline.add(bord);
		}
//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		if (retMap.size() == 0) {
			String skipStr = "{$skip:" + (pgNum * Constants.PAGE_SIZE) + "}}";
			Bson bskp = BasicDBObject.parse(skipStr);
			pipeline.add(bskp);

			String limit = "{$limit:" + Constants.PAGE_SIZE + "}}";
			Bson blim = BasicDBObject.parse(limit);
			pipeline.add(blim);
		}
//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		MongoDatabase mongo = mongoTemplate.getDb();
		AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
//			logger.debug(">>>>>>>>>>{}", doc);
			String key = doc.getString("_id");
			Map<String, Double> tmpMap = retMap.get(key);
			if (tmpMap == null)
				tmpMap = new HashMap<>();
			for (String kpi : kpis) {
				if (kpi.equalsIgnoreCase(Constants.ACTIVE_ITEMS)
						|| kpi.equalsIgnoreCase(Constants.ACTIVE_PRICE_AGREEMENT)
						|| kpi.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS))
					tmpMap.put(kpi, doc.getInteger(kpi).doubleValue());
				else
					tmpMap.put(kpi, doc.getDouble(kpi));
			}
			retMap.put(key, tmpMap);
		});

	}

	public void setCommonDateFiltersForPOToLimitFutureDate(List<Bson> pipeline) {
		Bson purgeDates1 = match(lte("invoiceDate", LocalDate.now().plusYears(5)));
		Bson purgeDates2 = match(lte("purchaseOrderCreationDate", LocalDate.now().plusYears(5)));

		pipeline.add(purgeDates1);
		pipeline.add(purgeDates2);
	}



}
