package com.kafka.learning.inventory_service.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FailedMessage {

    @Id
    private UUID id;

    private UUID eventId;
    private String payload;
    private String reason;
    private LocalDateTime failedAt;

    @Enumerated(EnumType.STRING)
    private FailedStatus status;
}
