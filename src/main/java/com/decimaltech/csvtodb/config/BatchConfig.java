package com.decimaltech.csvtodb.config;

import com.decimaltech.csvtodb.model.Account;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    // It will use the information of application.properties file:
    @Autowired
    private DataSource dataSource;

    // JobBuilderFactory help us to create the Job:
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    // SetBuilderFactory help us to create the Step:
    @Autowired
    private StepBuilderFactory stepBuilderFactory;


    //========create reader: ================//
    @Bean
    public FlatFileItemReader<Account> reader(){
        FlatFileItemReader<Account> reader = new FlatFileItemReader<>();
        // Location where to read the CSV file:
        reader.setResource (new ClassPathResource("1500000Records.csv"));
        // How to map the read line:
        reader.setLineMapper(getLineMapper());
        // If an any error comes then 1 line skip:
        reader.setLinesToSkip (1);
        // finally return the reader:
        return reader;
    }

    private LineMapper<Account> getLineMapper() {
        // DefaultLineMapper class is subClass of LineMapper Class:
        DefaultLineMapper<Account> lineMapper = new DefaultLineMapper<> ();

        // Which column you want read from csv file:
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer ();
        lineTokenizer.setNames(new String[]{"Date","Description","Deposits","Withdrawls","Balance"});
        lineTokenizer.setIncludedFields (new int[]{0,1,2,3,4});

        // From which class you want read the column:
        BeanWrapperFieldSetMapper<Account> fieldSetMapper = new BeanWrapperFieldSetMapper<> ();
        fieldSetMapper.setTargetType (Account.class);

        lineMapper.setLineTokenizer (lineTokenizer);
        lineMapper.setFieldSetMapper (fieldSetMapper);

        return lineMapper;

    }

    //============Create ItemProcessor=====================//
    @Bean
    public AccountItemProcessor processer(){
        return new AccountItemProcessor ();
    }


    //=================Create ItemWriter==================//
    @Bean
    public JdbcBatchItemWriter<Account> writer(){
        JdbcBatchItemWriter<Account> writer = new JdbcBatchItemWriter<> ();
        writer.setItemSqlParameterSourceProvider (new BeanPropertyItemSqlParameterSourceProvider<> ());
        writer.setSql ("insert into account(date, description, deposits, withdrawls, balance) values(:date, :description, :deposits, :withdrawls, :balance)");
        writer.setDataSource(this.dataSource);
        return writer;
    }

    //=================Create Job===============//
    @Bean
    public Job importAccountJob(){
        return (Job) this.jobBuilderFactory.get("ACCOUNT-IMPORT-JOB")
                .incrementer(new RunIdIncrementer())
                .listener (new JobCompletionNotificationListener ())
                .flow(step1())
                .end()
                .build();
    }

    //==================Create Step=============//
    @Bean
    public Step step1(){
        return this.stepBuilderFactory.get("step1")
                .<Account, Account>chunk(10000)
                .reader(reader())
                .processor(processer())
                .writer(writer ())
                .build();
    }
}
