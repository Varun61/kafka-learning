package com.kafka.learning.inventory_service.repository;

import com.kafka.learning.inventory_service.entity.FailedMessage;
import com.kafka.learning.inventory_service.entity.FailedStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface FailedMessageRepository extends JpaRepository<FailedMessage, UUID> {

    List<FailedMessage> findByStatus(FailedStatus status);
}
