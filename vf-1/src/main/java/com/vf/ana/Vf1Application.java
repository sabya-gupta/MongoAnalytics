package com.vf.ana;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//@Configuration
//@EnableMongoRepositories(basePackages={"com.vf.ana"}, mongoTemplateRef="serverMongoTemplate")
public class Vf1Application {

	public static void main(String[] args) {
		SpringApplication.run(Vf1Application.class, args);
	}

}
