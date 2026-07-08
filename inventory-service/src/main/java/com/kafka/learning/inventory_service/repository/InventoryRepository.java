package com.kafka.learning.inventory_service.repository;

import com.kafka.learning.inventory_service.entity.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InventoryRepository extends JpaRepository<Inventory, String> {
}
