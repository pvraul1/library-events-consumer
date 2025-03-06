package com.learnkafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by ij, Spring Framework Guru.
 *
 * @author architecture - rperezv
 * @version 06/03/2025 - 12:18
 * @since jdk 1.21
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RetryScheduler {

    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventsService libraryEventsService;

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {
        log.info("Retrying failed records started!");

        failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY)
                .forEach(failureRecord -> {
                    log.info("Processing failure record: {}", failureRecord);

                    var consumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventsService.processLibraryEvent(consumerRecord);

                        failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecords: {}", e.getMessage());
                    }
                });

        log.info("Retrying failed records Completed!");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getKey_value(),
                failureRecord.getErrorRecord()
        );
    }

}
