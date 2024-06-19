package com.gopal.springBatchProcessing.config;

import com.gopal.springBatchProcessing.entity.Customer;
import com.gopal.springBatchProcessing.repo.CustomerRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ItemWriterImplementaion implements ItemWriter<Customer> {
    @Autowired
    CustomerRepository customerRepository;
    @Override
    public void write(Chunk chunk) throws Exception {
        customerRepository.saveAll(chunk);
    }
}
