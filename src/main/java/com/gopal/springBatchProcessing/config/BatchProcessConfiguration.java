package com.gopal.springBatchProcessing.config;

import com.gopal.springBatchProcessing.entity.Customer;
import com.gopal.springBatchProcessing.repo.CustomerRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.task.ThreadPoolTaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Configuration
public class BatchProcessConfiguration {
    List<Customer> customers = new ArrayList<>();
//    int availableProcessors = Runtime.getRuntime().availableProcessors()/2;
    int availableProcessors = 4;

    @Bean
    public Job jobBean(JobRepository jobRepository, JobExecutionListener listener, @Qualifier(value = "masterStep") Step steps) {
        return new JobBuilder("spring-batch-gopal-postgres", jobRepository)
                .listener(listener)
                .start(steps)
                .build();
    }
    @Bean
    PartitionHandler partitionHandler(TaskExecutor taskExecutor, @Qualifier(value = "slaveStep") Step slaveStep){
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor);
        taskExecutorPartitionHandler.setGridSize(availableProcessors);
        taskExecutorPartitionHandler.setStep(slaveStep);
        return taskExecutorPartitionHandler;
    }

    @Bean(name = "slaveStep")
    public Step slaveSteps(TaskExecutor taskExecutor, JobRepository jobRepository, PlatformTransactionManager transactionManager, ItemReader reader, ItemProcessor processor, ItemWriter writer) {
        return new StepBuilder("slaveStep", jobRepository)
                .<Customer, Customer>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
//                .taskExecutor(taskExecutor)
                .build();
    }
    @Bean(name = "masterStep")
    public Step masterSteps(JobRepository jobRepository,Partitioner partitioner,PartitionHandler partitionHandler){
        return new StepBuilder("masterStep",jobRepository)
                .partitioner("slaveStep",partitioner)
                .partitionHandler(partitionHandler)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
//        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
//        asyncTaskExecutor.setConcurrencyLimit(10);
//        return asyncTaskExecutor;
//        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
//        int availableProcessors = Runtime.getRuntime().availableProcessors();
//        threadPoolTaskExecutor.setMaxPoolSize(availableProcessors);
//        threadPoolTaskExecutor.setCorePoolSize(availableProcessors/2);
//        threadPoolTaskExecutor.setQueueCapacity(availableProcessors*10);
//        return threadPoolTaskExecutor;

        return new ThreadPoolTaskExecutorBuilder()
                .maxPoolSize(availableProcessors)
                .corePoolSize(availableProcessors/2)
                .queueCapacity(availableProcessors*100)
                .build();
    }

    static int count = 0;

    @Bean
    public ItemProcessor<Customer, Customer> processor() {
//        ItemProcessor<Customer, Customer> itemProcessor = item -> item;
        ItemProcessor<Customer, Customer> itemProcessor = customer -> {
            customer.setId(UUID.randomUUID().toString());
            customers.add(customer);
            return customer;
        };
        return itemProcessor;


//       return new ItemProcessor<Customer, Customer>() {
//          @Override
//          public Customer process(Customer item) throws Exception {
//             return item;
//          }
//       };
    }
    // id,firstName,lastName,email,gender,contactNo
    @Bean
    public FlatFileItemReader reader() {
        return new FlatFileItemReaderBuilder<Customer>()
                .name("itemReader")
                .resource(new ClassPathResource("data.csv"))
                .linesToSkip(1)
//                .lineMapper(lineMapper())
                .delimited()
                .delimiter(",")
                .names("id", "first_name", "last_name", "email", "gender", "contactNo")
                .targetType(Customer.class)
                .build();
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "first_name", "last_name", "email", "gender", "contact_no");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

//    @Bean
    public ItemWriter<Customer> writer(CustomerRepository customerRepository) {
//        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
//        writer.setRepository(customerRepository);
//        writer.setMethodName("save");
//        return writer;
        return new RepositoryItemWriterBuilder<Customer>()
                .repository(customerRepository)
                .methodName("save")
                .build();
    }

}

