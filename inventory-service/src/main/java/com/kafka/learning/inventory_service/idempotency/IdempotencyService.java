package com.kafka.learning.inventory_service.idempotency;

import java.util.UUID;

public interface IdempotencyService {

    boolean isDuplicate(UUID eventId);

    void markProcessed(UUID eventId);
}
