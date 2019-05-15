package com.example.bk.writer;

import com.example.bk.Customer;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
@RequiredArgsConstructor
@EnableBatchProcessing
public class WriterApplication {

	private final StepBuilderFactory stepBuilder;
	private final JobBuilderFactory jobBuilder;
	private final KafkaTemplate<Long, Customer> template;

	@Bean
	Job job() {
		return this.jobBuilder
			.get("files-to-kafka")
			.start(fileToKafka())
			.incrementer(new RunIdIncrementer())
			.build();
	}

	@Bean
	KafkaItemWriter<Long, Customer> kafkaItemWriter() {
		var ids = new AtomicLong();
		return new KafkaItemWriterBuilder<Long, Customer>()
			.itemKeyMapper(new Converter<Customer, Long>() {
				@Override
				public Long convert(Customer customer) {
					return ids.incrementAndGet();
				}
			})
			.kafkaTemplate(template)
			.build();
	}

	@Bean
	Step fileToKafka() {

		var reader = new ItemReader<Customer>() {

			private final AtomicLong counter = new AtomicLong();

			@Override
			public Customer read() {
				if (this.counter.incrementAndGet() < 10_100) {
					return new Customer(UUID.randomUUID().toString(), name());
				}
				return null;
			}
		};

		return this.stepBuilder
			.get("s1")
			.<Customer, Customer>chunk(10)
			.reader(reader)
			.writer(kafkaItemWriter())
			.build();
	}

	String name() {
		return Math.random() > .5 ? "Jane" : "John";
	}

	public static void main(String args[]) {
		SpringApplication.run(WriterApplication.class, args);
	}
}
