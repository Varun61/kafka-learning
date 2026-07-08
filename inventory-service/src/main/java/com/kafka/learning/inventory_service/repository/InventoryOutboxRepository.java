package com.kafka.learning.inventory_service.repository;

import com.kafka.learning.inventory_service.entity.InventoryOutboxEvent;
import com.kafka.learning.inventory_service.entity.InventoryOutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface InventoryOutboxRepository extends JpaRepository<InventoryOutboxEvent, UUID> {

    List<InventoryOutboxEvent> findByStatus(InventoryOutboxStatus status);
}
