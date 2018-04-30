package com.nullendpoint.batch;

import javax.sql.DataSource;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import java.util.ArrayList;
import java.util.List;

@Configuration
@PropertySource("classpath:batch.properties")
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Value("${openshift.project.name}")
    public String openshiftProjectName;

    @Value("${openshift.pvc.name}")
    public String openshiftClaimName;

//
//    @Autowired
//    public DataSource dataSource;

    private static final String OVERRIDDEN_BY_EXPRESSION = null;

    // tag::readerwriterprocessor[]
    @Bean
    @StepScope
    public FlatFileItemReader<Person> reader(@Value("#{jobParameters[jobFileLocation]}") String jobFileLocation) {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new FileSystemResource(jobFileLocation));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }


    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer() {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<Person>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setDataSource(dataSource());
        return writer;
    }
    // end::readerwriterprocessor[]

    // tag::jobstep[]
    @Bean
    public Job importUserJob(JobCompletionNotificationListener jobCompletionNotificationListener) {
        return jobBuilderFactory.get("importUserJob")
                .validator(new DefaultJobParametersValidator(
                        new String[]{"jobFileLocation"},new String[]{}))
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener)
                .flow(step1()).next(step2())
                .end()
                .build();
    }

    @Bean
    public JobCompletionNotificationListener jobCompletionNotificationListener(DataSource dataSource){
        return new JobCompletionNotificationListener(new JdbcTemplate(dataSource));
    }

    @Bean
    public Step step1(){
        return stepBuilderFactory.get("step1").tasklet(generateCsvFile(OVERRIDDEN_BY_EXPRESSION)).build();
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("step2")
                .<Person, Person> chunk(10)
                .reader(reader(OVERRIDDEN_BY_EXPRESSION))
                .processor(processor())
                .writer(writer())
                .build();
    }
    // end::jobstep[]


    @Bean
    @StepScope
    public OpenshiftJobTasklet generateCsvFile(@Value("#{jobParameters[jobFileLocation]}") String jobFileLocation)
    {
        List<EnvVar> evbList = new ArrayList<EnvVar>();
        EnvVarBuilder evb = new EnvVarBuilder();
        evb.withName("FILENAME");
        evb.withValue(jobFileLocation);
        evbList.add(evb.build());

        return new OpenshiftJobTasklet(openshiftProjectName, "generate-csv",
                "registry.access.redhat.com/rhel7/rhel:latest",
                openshiftClaimName, evbList,
                "/bin/bash", "-c", "curl -o /var/tmp/create-csv.sh -s https://gist.githubusercontent.com/welshstew/561cf55cfea4d22b9fc9c4944f1da952/raw/13a1cffec13549d1582a48fa63628d91c7bb9565/create-csv.sh; source /var/tmp/create-csv.sh");
    }


    @Bean
    public JobRepository jobRepository(DataSource dataSource, DataSourceTransactionManager dataSourceTransactionManager) throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDatabaseType("POSTGRES");
        factory.setDataSource(dataSource);
        factory.setTransactionManager(dataSourceTransactionManager);
        return factory.getObject();
    }


    @Bean
    public JobLauncher jobLauncher(JobRepository jobRepository) {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        return jobLauncher;
    }

    @Bean
    @ConfigurationProperties(prefix="spring.datasource")
    public DataSource dataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public DataSourceTransactionManager dataSourceTransactionManager(DataSource dataSource){
        return new DataSourceTransactionManager(dataSource);
    }




}
