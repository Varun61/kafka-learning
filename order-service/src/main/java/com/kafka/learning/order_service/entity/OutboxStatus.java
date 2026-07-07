package com.kafka.learning.order_service.entity;

public enum OutboxStatus {
    PENDING,
    SENT,
    FAILED
}
