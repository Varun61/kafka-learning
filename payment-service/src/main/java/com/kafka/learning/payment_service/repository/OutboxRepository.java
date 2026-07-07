package com.kafka.learning.payment_service.repository;

import com.kafka.learning.payment_service.entity.OutboxEvent;
import com.kafka.learning.payment_service.entity.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {
    List<OutboxEvent> findByStatus(OutboxStatus status);
}
