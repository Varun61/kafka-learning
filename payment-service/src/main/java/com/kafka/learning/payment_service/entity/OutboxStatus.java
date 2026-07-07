package com.kafka.learning.payment_service.entity;

public enum OutboxStatus {
    PENDING,
    SENT,
    FAILED
}