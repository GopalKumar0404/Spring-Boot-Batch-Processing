package com.gopal.springBatchProcessing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/*
 *
 * @author Gopal Kumar
 */

@SpringBootApplication
public class SpringBatchProcessingApplication {

    public static void main(String[] args) {
//		SpringApplication.run(SpringBatchProcessingApplication.class, args);
        System.exit(SpringApplication.exit(SpringApplication.run(SpringBatchProcessingApplication.class, args)));
    }

}
