package com.ozymern.spring.azure.storage;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringAzureStorageApplication   {

    @Autowired


    public static void main(String[] args) {
        SpringApplication.run(SpringAzureStorageApplication.class, args);
    }

}
