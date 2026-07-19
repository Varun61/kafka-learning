package com.kafka.learning.inventory_service.repository;

import com.kafka.learning.inventory_service.entity.Inventory;
import io.lettuce.core.dynamic.annotation.Param;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.util.Optional;

public interface InventoryRepository extends JpaRepository<Inventory, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT i From Inventory i WHERE i.item = :item")
    Optional<Inventory> findByItemForUpdate(@Param("item") String item);
}
