package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * LibrayEventsRetryConsumer
 * <p>
 * Created by IntelliJ, Spring Framework Guru.
 *
 * @author architecture - pvraul
 * @version 03/03/2025 - 11:23
 * @since 1.17
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LibrayEventsRetryConsumer {

    private final LibraryEventsService libraryEventsService;

    @KafkaListener(topics = {"${topics.retry}"},
            autoStartup = "${retryListener.startup:false}",
            groupId = "retry-listener-group"
    )
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

        log.info("ConsumerRecord in Retry Consumer : {}", consumerRecord);
        consumerRecord.headers()
                        .forEach(header -> {
                            log.info("Header Key: {}, Header Value: {}", header.key(), new String(header.value()));
                        });

        libraryEventsService.processLibraryEvent(consumerRecord);

    }

}
