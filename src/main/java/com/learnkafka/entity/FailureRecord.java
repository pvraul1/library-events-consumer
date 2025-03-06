package com.learnkafka.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by ij, Spring Framework Guru.
 *
 * @author architecture - rperezv
 * @version 06/03/2025 - 10:06
 * @since jdk 1.21
 */
@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FailureRecord {

    @Id
    @GeneratedValue
    private Integer id;

    private String topic;
    private String key_value;
    private String errorRecord;
    private Integer partition;
    private Long offset_value;
    private String exception;
    private String status;

}
