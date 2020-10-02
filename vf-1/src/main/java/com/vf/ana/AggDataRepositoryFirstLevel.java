package com.vf.ana;

import java.util.List;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.repository.MongoRepository;

import com.vf.ana.ent.AggregatedDataFirstLevel;

public interface AggDataRepositoryFirstLevel extends MongoRepository<AggregatedDataFirstLevel, String> {

	List<AggregatedDataFirstLevel> findAllByPropName(String dimName, PageRequest pg, Sort sort);
	List<AggregatedDataFirstLevel> findAllByPropName(String dimName);

	int countByPropName(String propName);
}
