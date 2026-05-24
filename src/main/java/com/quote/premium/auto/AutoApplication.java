package com.quote.premium.auto;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableFeignClients
//@EnableKafka
public class AutoApplication {
	public static void main(String[] args) {
		SpringApplication.run(AutoApplication.class, args);
	}

}
