package com.gopal.springBatchProcessing.config;

import com.gopal.springBatchProcessing.entity.Customer;
import com.gopal.springBatchProcessing.repo.CustomerRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.ThreadLocalRandom;

@Configuration
public class BatchProcessConfiguration {

    @Bean
    public Job jobBean(JobRepository jobRepository, JobExecutionListener listener, Step steps) {
        return new JobBuilder("Job", jobRepository)
                .listener(listener)
                .start(steps)
                .build();
    }

    @Bean
    public Step steps(TaskExecutor taskExecutor, JobRepository jobRepository, PlatformTransactionManager transactionManager, ItemReader reader, ItemProcessor processor, ItemWriter writer) {
        return new StepBuilder("jobStep", jobRepository)
                .<Customer, Customer>chunk(100000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }
    static int count=0;
    @Bean
    public ItemProcessor<Customer, Customer> processor() {
//        ItemProcessor<Customer, Customer> itemProcessor = item -> item;
        ItemProcessor<Customer, Customer> itemProcessor = customer -> {
//            System.out.println(customer);
            customer.setId((count++)+"Gopal"+ ThreadLocalRandom.current().nextInt(0,999999999)+ThreadLocalRandom.current().nextLong(-999999999,1000000000));
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

    @Bean
    public FlatFileItemReader reader() {
        return new FlatFileItemReaderBuilder<Customer>()
                .name("itemReader")
                .resource(new ClassPathResource("data.csv"))
                .linesToSkip(1)
//                .lineMapper(lineMapper())
                .delimited()
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

    @Bean
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
