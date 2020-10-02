package com.vf.ana;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.vf.ana.ent.AggregatedDataFirstLevel;

@Repository
public class AggBatchSecondLevel {

	@Autowired
	ComboRepositoryPASDPOID comboRepositoryPASDPOID;
	
	@Autowired
	PASDAnaRepository pASDAnaRepository;
	
	
	Logger logger = LoggerFactory.getLogger(getClass());
	
	
	public void generateAggregatedCollection() {
		
		Map<String, Map<String, Double>> ret = comboRepositoryPASDPOID.getAllMeasuresFromPOIDForAPropertyForAllPropValues();
	}	
	
//	private void generateAggregatedCollectionOld(String propName) {
//		
//		Map<String, Map<String, Double>> ret = comboRepositoryPASDPOID.getAllMeasuresFromPOIDForAPropertyForAllPropValues(propName);
//		Map<String, AggregatedDataFirstLevel> map = new HashMap<>();
//		for(String key : ret.keySet()) {
//			AggregatedDataFirstLevel obj = new AggregatedDataFirstLevel();
//			obj.setAggregatedDataFirstLevelId(UUID.randomUUID().toString());
//			obj.setPropName(propName);
//			obj.setPropVal(key);
//			obj.setOrderValue(ret.get(key).get(Constants.ORDER_VALUE));
//			obj.setInvoiceValue(ret.get(key).get(Constants.INVOICE_VALUE));
//			obj.setOrdersIssued(ret.get(key).get(Constants.NUMBER_OF_ORDERS));
//			map.put(key, obj);
//		}
//		Map<String, Map<String, Double>> ret2 = comboRepositoryPASDPOID.getAllMeasuresFromPASDForAPropertyForAllPropValues(propName);
//		for(String key : ret2.keySet()) {
//			AggregatedDataFirstLevel obj = map.get(key);
//			if(obj==null) {
//				obj = new AggregatedDataFirstLevel();
//				obj.setAggregatedDataFirstLevelId(UUID.randomUUID().toString());
//				obj.setPropName(propName);
//				obj.setPropVal(key);
//			}
//			obj.setActiveItems(ret2.get(key).get(Constants.ACTIVE_ITEMS));
//			obj.setActivePriceAgreements(ret2.get(key).get(Constants.ACTIVE_PRICE_AGREEMENT));
//			map.put(key, obj);
//		}
//
//		logger.debug("SIZE = {}", map.keySet().size());
//
//		
//		int size = 10;
//		int batchSize = 250;
//		
//		ExecutorService executor = Executors.newFixedThreadPool(size);
//		
//		List<Callable<String>> lstRunnable = new ArrayList<>();
//		int cntr = 0;
//		List<AggregatedDataFirstLevel> lst = null;
//		for(String key : map.keySet()) {
////			logger.debug("cntr = {}", cntr);
//			if(cntr%batchSize ==0) {
//				if(lst!=null) {
//					logger.debug("added>>> = {}", cntr);
//					PushCallable pr = new PushCallable(lst, cntr, aggDataRepositoryFirstLevel);
//					lstRunnable.add(pr);
//				}
//				lst = new ArrayList<AggregatedDataFirstLevel>();				
//				lst.add(map.get(key));
//				
//				cntr++;
//			}else {
//				lst.add(map.get(key));
//				cntr++;
//			}
//		}
//		if(lst!=null) {
//			PushCallable pr = new PushCallable(lst, cntr, aggDataRepositoryFirstLevel);
//			lstRunnable.add(pr);
//		}
//		
//		logger.debug(">>>>{}", lstRunnable.size());
//		
//		try {
//			executor.invokeAll(lstRunnable);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//	}
	
	
}
