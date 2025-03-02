package com.learnkafka.config;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;
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

    private final KafkaProperties properties;

    public DefaultErrorHandler errorHandler() {
        var exceptionToIgnoreList = List.of(IllegalArgumentException.class);
        var exceptionToRetryList = List.of(RecoverableDataAccessException.class);
        var fixedBackOff = new FixedBackOff(1000L, 2);
        var expBackOff  = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);
        var errorHandler = new DefaultErrorHandler(
                // fixedBackOff
                expBackOff
        );

        exceptionToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
        exceptionToRetryList.forEach(errorHandler::addRetryableExceptions);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception: {}, deliveryAttempt: {}", ex.getMessage(), deliveryAttempt);
        });

        return errorHandler;
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
