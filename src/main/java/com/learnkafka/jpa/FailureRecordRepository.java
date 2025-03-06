package com.learnkafka.jpa;

import com.learnkafka.entity.FailureRecord;
import org.springframework.data.repository.CrudRepository;

/**
 * Created by ij, Spring Framework Guru.
 *
 * @author architecture - raulp
 * @version 06/03/2025 - 10:09
 * @since jdk 1.21
 */
public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {
}
