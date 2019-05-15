package com.example.bk.reader;

import com.example.bk.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Properties;

@Log4j2
@EnableBatchProcessing
@SpringBootApplication
@RequiredArgsConstructor
public class ReaderApplication {


	public static void main(String[] args) {
		SpringApplication.run(ReaderApplication.class, args);
	}

	private final StepBuilderFactory stepBuilder;
	private final JobBuilderFactory jobBuilder;
	private final KafkaProperties kp;

	@Bean
	Step one() {

		var writer = new ItemWriter<Customer>() {
			@Override
			public void write(List<? extends Customer> items) throws Exception {
				items.forEach(log::info);
			}
		};
		return this.stepBuilder
			.get("one")
			.<Customer, Customer>chunk(10)
			.reader(this.kafkaItemReader())
			.writer(writer)
			.build();
	}


	@Bean
	KafkaItemReader<String, Customer> kafkaItemReader() {

		var properties = new Properties();
		properties.putAll(kp.buildConsumerProperties());

		return new KafkaItemReaderBuilder<String, Customer>()
			.topic("customers")
			.consumerProperties(properties)
			.saveState(true)
			.partitions(0)
			.name("inbound-customers")
			.build();
	}


	@Bean
	Job job() throws Exception {
		return this.jobBuilder
			.get("job")
			.start(one())
			.incrementer(new RunIdIncrementer())
			.build();
	}


}

