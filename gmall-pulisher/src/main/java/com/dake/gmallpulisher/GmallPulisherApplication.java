package com.dake.gmallpulisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.dake.gmallpulisher.mapper")
public class GmallPulisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(GmallPulisherApplication.class, args);
	}

}