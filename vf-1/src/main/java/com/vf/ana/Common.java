package com.vf.ana;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;

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

@Component
public class Common {

	Logger logger = LoggerFactory.getLogger(getClass());

	public String formMatchClauseForFilter2(final Map<String, String> filterMap, final String dateFilterName, final String date) {

		if (filterMap == null)
			return null;

		final StringBuilder expr = new StringBuilder();

		final StringBuilder comma = new StringBuilder();

		filterMap.keySet().forEach(key -> {
			final String filter = "{" + key + " : '" + filterMap.get(key) + "'}";
			logger.debug("filter = {}", filter);
			expr.append(comma).append(filter);
			if (comma.length() == 0)
				comma.append(" , ");
		});

		logger.debug(">>>>expr = {}", expr);

		if (date != null) {
			final String startDate = date;
			LocalDate dt = LocalDate.parse(date, DateTimeFormatter.ISO_DATE);
			dt = dt.plusDays(1);
			final String endDate = dt.format(DateTimeFormatter.ISO_DATE);
			final String filter = "{" + dateFilterName + ":{$gte:ISODate('" + startDate + "')}}";
			expr.append(comma).append(filter);
			logger.debug("filter = {} expr = {}", filter, expr);

			final String filter2 = "{" + dateFilterName + ":{$lte:ISODate('" + endDate + "')}}";
			logger.debug("filter = {} expr = {}", filter2, expr);
			expr.append(comma).append(filter2);
		}
		// {validFromDate:{$lte:ISODate('2020-09-06')}}
		logger.debug("expr = {}", expr);
		final String qMatch = "{$match:{$and:[" + expr.toString() + "]}}";

		return qMatch;
	}

	public String formMatchClauseForListFilter(final Map<String, List<String>> filterMap, final String type, final List<String> dates) {

		if (filterMap == null)
			return null;

		final StringBuilder expr = new StringBuilder();

		final StringBuilder comma = new StringBuilder();

		filterMap.keySet().forEach(key -> {
			final String filter = "{" + key + " : { $in: " + filterMap.get(key).toString() + "}}";
			logger.debug("filter = {}", filter);
			expr.append(comma).append(filter);
			if (comma.length() == 0)
				comma.append(" , ");
		});

		logger.debug("expr = {}", expr);
		final String qMatch = "{$match:{$and:[" + expr.toString() + "]}}";
		logger.debug(">>>>expr = {}", qMatch);

		return qMatch;
	}

	public Bson formMatchClauseForListFilterBson(final Map<String, List<String>> filterMap) {

		if (filterMap == null)
			return null;

		final List<Bson> andFilters = new ArrayList<>();

		for (final String key : filterMap.keySet()) {
			final Bson filter = in(key, filterMap.get(key));
			andFilters.add(filter);
		}

		return match(and(andFilters));

	}

	public Bson getYYYYMMFilterForPAandAI(final List<String> yyyymm) {
		final List<Bson> dateFilterList = new ArrayList<>();
		for (final String ym : yyyymm) {
			final LocalDate dt = LocalDate.parse(ym + "-01", DateTimeFormatter.ISO_DATE);
			final Bson df = and(lte("validFromDate", dt), gte("validToDate", dt));
			dateFilterList.add(df);
		}
		final Bson orDf = match(or(dateFilterList));
		return orDf;
	}
	
	
	
	public int getCount(final String collection, final List<Bson> pipeline, final MongoTemplate mongoTemplate) {

		final List<Bson> newpipeline = new ArrayList<>();
		newpipeline.addAll(pipeline);

		final String cntStr = "{$group:{_id:null, val:{'$sum':1}}}";
		newpipeline.add(BasicDBObject.parse(cntStr));

		final MongoDatabase mongo = mongoTemplate.getDb();

		int count = 0;

		try {
//			printDocs(collection, pipeline);
//			printDocs(collection, newpipeline);
			count = mongo.getCollection(collection).aggregate(newpipeline).first().getInteger("val");
		} catch (final NullPointerException e) {

		} catch (final Exception e) {
			e.printStackTrace();
		}

		return count;
	}

	public void printDocs(final String collection, final List<Bson> pipeline, final MongoTemplate mongoTemplate) {
		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug(">>>>>>>>>>{}", doc);
		});

	}

	public void printDocs(final String iden, final String collection, final List<Bson> pipeline,
			final MongoTemplate mongoTemplate) {
		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug("<><>{}-{}", iden, doc);
		});

	}

	public void getResults(final List<Bson> pipeline, final int dir, final int pgNum, final Map<String, Map<String, Double>> retMap,
			final List<String> kpis, final String collection, final String sortByField, final MongoTemplate mongoTemplate) {

		if (sortByField != null && dir == Constants.SORT_DIRECTION_ASCENDING && retMap.size() == 0) {
			final String ordStr = "{$sort:{" + sortByField + ":1}}";
			final Bson bord = BasicDBObject.parse(ordStr);
			pipeline.add(bord);
		}
//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		if (sortByField != null && dir == Constants.SORT_DIRECTION_DESCENDING && retMap.size() == 0) {
			final String ordStr = "{$sort:{" + sortByField + ":-1}}";
			final Bson bord = BasicDBObject.parse(ordStr);
			pipeline.add(bord);
		}
//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		if (retMap.size() == 0) {
			final String skipStr = "{$skip:" + (pgNum * Constants.PAGE_SIZE) + "}";
			final Bson bskp = BasicDBObject.parse(skipStr);
			pipeline.add(bskp);

			final String limit = "{$limit:" + Constants.PAGE_SIZE + "}";
			final Bson blim = BasicDBObject.parse(limit);
			pipeline.add(blim);
		}
//		printDocs(Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME, pipeline);

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collection).aggregate(pipeline);

		ret.cursor().forEachRemaining(doc -> {
			logger.debug(">>>>>>>>>>{}", doc);
			final String key = doc.getString("_id");
			Map<String, Double> tmpMap = retMap.get(key);
			if (tmpMap == null)
				tmpMap = new HashMap<>();
			for (final String kpi : kpis) {
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

	public void setCommonDateFiltersForPOToLimitFutureDate(final List<Bson> pipeline) {
		final Bson purgeDates1 = match(lte("invoiceDate", LocalDate.now().plusYears(5)));
		final Bson purgeDates2 = match(lte("purchaseOrderCreationDate", LocalDate.now().plusYears(5)));

		pipeline.add(purgeDates1);
		pipeline.add(purgeDates2);
	}



}
