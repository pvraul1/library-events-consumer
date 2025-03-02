package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

/**
 * LibraryEventsService
 * <p>
 * Created by IntelliJ, Spring Framework Guru.
 *
 * @author architecture - pvraul
 * @version 01/03/2025 - 10:46
 * @since 1.17
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LibraryEventsService {

    private final LibraryEventsRepository libraryEventsRepository;

    private final ObjectMapper objectMapper;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

        LibraryEvent libraryEvent = this.objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("Library Event : {}", libraryEvent);

        if (libraryEvent != null && libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 999) {
            throw new RecoverableDataAccessException("Tempory Network Issue");
        }

        switch (Objects.requireNonNull(libraryEvent).getLibraryEventType()) {
            case NEW:
                // save operation
                save(libraryEvent);
                break;
            case UPDATE:
                // validate the library event
                validate(libraryEvent);
                // update operation
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library Event Id is missing");
        }
        this.libraryEventsRepository.findById(libraryEvent.getLibraryEventId())
                .orElseThrow(() -> new IllegalArgumentException("Not a valid Library Event"));
        log.info("Validation is successful for the Library Event : {}", libraryEvent);
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        this.libraryEventsRepository.save(libraryEvent);
        log.info("Successfully Persisted the Library Event {}", libraryEvent);
    }

}
