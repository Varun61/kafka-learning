package com.kafka.learning.inventory_service.entity;

public enum InventoryOutboxStatus {
    PENDING,
    SENT,
    FAILED
}
