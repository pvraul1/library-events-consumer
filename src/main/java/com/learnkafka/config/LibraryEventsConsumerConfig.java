package com.learnkafka.config;

import com.learnkafka.service.FailureService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

/**
 * LibraryEventsConsumerConfig
 * <p>
 * Created by IntelliJ, Spring Framework Guru.
 *
 * @author architecture - pvraul
 * @version 27/02/2025 - 09:34
 * @since 1.17
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";
    public static final String SUCCESS = "SUCCESS";

    @Autowired
    private KafkaProperties properties;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private FailureService failureService;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                }
        );

        return recoverer;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, consumerRecord);
        var record = (ConsumerRecord<Integer, String>) consumerRecord;
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside the recoverable logic");
            failureService.saveFailedRecord(record, exception, RETRY);
        } else {
            log.info("Inside the non recoverable logic");
            failureService.saveFailedRecord(record, exception, DEAD);
        }
    };

    public DefaultErrorHandler errorHandler() {

        var exceptiopnToIgnorelist = List.of(
                IllegalArgumentException.class
        );
        var fixedBackOff = new FixedBackOff(1000L, 2L);

        ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);

        var defaultErrorHandler = new DefaultErrorHandler(
                //publishingRecoverer(),
                consumerRecordRecoverer,
                fixedBackOff
                // expBackOff
        );

        exceptiopnToIgnorelist.forEach(defaultErrorHandler::addNotRetryableExceptions);
        defaultErrorHandler.setRetryListeners(
                (record, ex, deliveryAttempt) ->
                        log.info("Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt)
        );

        return defaultErrorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());

        return factory;
    }

}
