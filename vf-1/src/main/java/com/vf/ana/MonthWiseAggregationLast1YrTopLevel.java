package com.vf.ana;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

@Repository
public class MonthWiseAggregationLast1YrTopLevel {
	@Autowired
	MongoTemplate mongoTemplate;

	@Autowired
	Common common;

	Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	TopLevelAnalyticsUtilPAandActiveItems topLevelAnalyticsUtilPAandActiveItems;

	public Map<String, Double> aggregateMonthWiseForLastOneYear(final String kPIName, final Map<String, List<String>> filters) {

		String collectionName = null;
		
		final Map<String, Double> retMap = new TreeMap<>();

		LocalDate endDate = LocalDate.now();
		final LocalDate endDateComp = LocalDate.now();
		final String strEndDate = endDate.format(DateTimeFormatter.ISO_DATE);
		final LocalDate startDateComp = endDate.minusDays(365);
		final LocalDate startDate = endDate.minusDays(365);
		final String strStartDate = startDate.format(DateTimeFormatter.ISO_DATE);

		
		
		final List<String> months = new ArrayList<>();
		final List<LocalDate> lastDays = new ArrayList<>();
		final List<LocalDate> firstDays = new ArrayList<>();

		while (endDate.compareTo(startDate) >= 0) {
			final String yymm = endDate.format(DateTimeFormatter.ofPattern("YYYY-MM"));
			final LocalDate ld = YearMonth.from(endDate).atEndOfMonth();
			final LocalDate firstDay = YearMonth.from(endDate).atDay(1);
			if (!lastDays.contains(ld))
				lastDays.add(ld);
			if (!months.contains(yymm)) {
				months.add(yymm);
			}
			if(!firstDays.contains(firstDay)) {
				firstDays.add(firstDay);
			}
			endDate = endDate.minusDays(25);
		}

		final List<Bson> pipeLine = new ArrayList<>();

		if (filters != null && filters.size()>0) {
			pipeLine.add(common.formMatchClauseForListFilterBson(filters));
		}

		
		if (kPIName.equalsIgnoreCase(Constants.ORDER_VALUE) || kPIName.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS)) {
			
			collectionName = Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME;
			
			final Bson matchClause = Aggregates.match(Filters.and(Filters.gte("purchaseOrderCreationDate", startDateComp), Filters.lte("purchaseOrderCreationDate", endDateComp)));
			pipeLine.add(matchClause);
		}else if(kPIName.equalsIgnoreCase(Constants.INVOICE_VALUE)) {
			
			collectionName = Constants.PURCHASE_ORDER_INVOICE_DATA_COLLECTION_NAME;
			
			final Bson matchClause = Aggregates.match(Filters.and(Filters.gte("invoiceDate", startDateComp), Filters.lte("invoiceDate", endDateComp)));
			pipeLine.add(matchClause);
		}else if(kPIName.equalsIgnoreCase(Constants.ACTIVE_ITEMS)) {
			
			for(final LocalDate date1 : firstDays) {
				final int cnt = topLevelAnalyticsUtilPAandActiveItems.getTotalActivePAsORActiveItemsByDate(date1.format(DateTimeFormatter.ISO_DATE), filters, false);
				retMap.put(date1.format(DateTimeFormatter.ofPattern("yyyy-MM")), (double) cnt);
			}
			logger.debug("{}", retMap);
			return retMap;
			
		}else if(kPIName.equalsIgnoreCase(Constants.ACTIVE_PRICE_AGREEMENT)) {
			
			for(final LocalDate date1 : firstDays) {
				final int cnt = topLevelAnalyticsUtilPAandActiveItems.getTotalActivePAsORActiveItemsByDate(date1.format(DateTimeFormatter.ISO_DATE), filters, true);
				retMap.put(date1.format(DateTimeFormatter.ofPattern("yyyy-MM")), (double) cnt);
			}
			logger.debug("{}", retMap);
			return retMap;
			
		}
		


		if(kPIName.equalsIgnoreCase(Constants.ORDER_VALUE)) {
			final String q2 = "{$project:{podate:{$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }}, value:{$multiply:[{$divide:['$netPricePOPrice', '$priceUnitPo']}, '$quantityOrderedPurchaseOrder']}   }}";
			final Bson bson2 = BasicDBObject.parse(q2);
			pipeLine.add(bson2);
			
			common.printDocs(collectionName, pipeLine, mongoTemplate);

			final String q3 = "{$group:{_id:'$podate', value:{$sum:'$value'}}}";
			final Bson bson3 = BasicDBObject.parse(q3);
			pipeLine.add(bson3);

			common.printDocs(collectionName, pipeLine, mongoTemplate);

			final String q4 = "{$sort:{_id:1}}";
			final Bson bson4 = BasicDBObject.parse(q4);
			pipeLine.add(bson4);

			common.printDocs(collectionName, pipeLine, mongoTemplate);
		}else if (kPIName.equalsIgnoreCase(Constants.INVOICE_VALUE)){
		
			final String q2 = "{$project:{podate:{$dateToString: { format: '%Y-%m', date: '$invoiceDate' }}, value:{$multiply:[{$divide:['$invoiceUnitPriceAsPerTc', '$priceUnitPo']}, '$invoiceQuantity']}   }}";
			final Bson bson2 = BasicDBObject.parse(q2);
			pipeLine.add(bson2);
		
			final String q3 = "{$group:{_id:'$podate', value:{$sum:'$value'}}}";
			final Bson bson3 = BasicDBObject.parse(q3);
			pipeLine.add(bson3);
		
			final String q4 = "{$sort:{_id:1}}";
			final Bson bson4 = BasicDBObject.parse(q4);
			pipeLine.add(bson4);

		} else if (kPIName.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS)) {
			final String q2 = "{$project:{podate:{$dateToString: { format: '%Y-%m', date: '$purchaseOrderCreationDate' }}, ponum:'$purchaseOrderNumberOne'   }}";
			final Bson bson2 = BasicDBObject.parse(q2);
			pipeLine.add(bson2);

			common.printDocs(collectionName, pipeLine, mongoTemplate);

			final String q3 = "{$group:{_id:{podate: '$podate', ponum:'$ponum'}, value:{$sum: 1}}}";
			final Bson bson3 = BasicDBObject.parse(q3);
			pipeLine.add(bson3);

			common.printDocs(collectionName, pipeLine, mongoTemplate);

			final String q3A = "{$group:{_id:'$_id.podate', value:{$sum: 1}}}";
			final Bson bson3A = BasicDBObject.parse(q3A);
			pipeLine.add(bson3A);

			common.printDocs(collectionName, pipeLine, mongoTemplate);
			logger.debug("---------------------------------------------");

			final String q4 = "{$sort:{_id:1}}";
			final Bson bson4 = BasicDBObject.parse(q4);
			pipeLine.add(bson4);

			common.printDocs(collectionName, pipeLine, mongoTemplate);
		}
		
		logger.debug("123{}", retMap);

		final MongoDatabase mongo = mongoTemplate.getDb();
		final AggregateIterable<Document> ret = mongo.getCollection(collectionName)
				.aggregate(pipeLine);

		ret.cursor().forEachRemaining(doc -> {
			final String key = doc.getString("_id");

			if (months.contains(key)) {
				final Map<String, Double> hm = new HashMap<>();
				if (kPIName.equalsIgnoreCase(Constants.NUMBER_OF_ORDERS))
					retMap.put(doc.getString("_id"), doc.getInteger("value").doubleValue());
				else
					retMap.put(doc.getString("_id"), doc.getDouble("value"));
			}

		});

		logger.debug("{}", retMap);
		return retMap;

	}

}
