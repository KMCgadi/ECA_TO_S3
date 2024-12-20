package com.s3.eca2;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Eca2Application {

	public static void main(String[] args) {
//		System.setProperty("hadoop.home.dir", "D:\\util\\hadoop-3.3.6"); // 리눅스 배포시 하둡이 설치된 실제 디렉토리 경로로 변경해야함
		System.setProperty("hadoop.home.dir", "/home/devoheca/hadoop/hadoop-3.3.6");
		SpringApplication.run(Eca2Application.class, args);
	}

}
