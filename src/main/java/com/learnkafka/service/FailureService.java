package com.learnkafka.service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

/**
 * Created by ij, Spring Framework Guru.
 *
 * @author architecture - rperezv
 * @version 06/03/2025 - 10:14
 * @since jdk 1.21
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class FailureService {

    private final FailureRecordRepository failureRecordRepository;

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String status) {

        var failureRecord = FailureRecord.builder()
                .topic(record.topic())
                .key_value(String.valueOf(record.key()))
                .errorRecord(record.value())
                .partition(record.partition())
                .offset_value(record.offset())
                .exception(exception.getCause().getMessage())
                .status(status)
                .build();

        failureRecordRepository.save(failureRecord);
    }
}
