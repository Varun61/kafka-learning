package com.kafka.learning.inventory_service.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Inventory {

    @Id
    private String item;

    private int availableQuantity;

    @Version
    private Long version; // for optimistic locking
}
