package com.kafka.learning.inventory_service.idempotency;

import com.kafka.learning.inventory_service.entity.ProcessedEvent;
import com.kafka.learning.inventory_service.repository.ProcessedEventRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class DatabaseIdempotencyService implements IdempotencyService {

    private final ProcessedEventRepository processedEventRepository;

    public DatabaseIdempotencyService(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @Override
    public boolean isDuplicate(UUID eventId) {
        return processedEventRepository.existsById(eventId);
    }

    @Override
    public void markProcessed(UUID eventId) {
        ProcessedEvent processedEvent = new ProcessedEvent(
                eventId,
                LocalDateTime.now()
        );
        processedEventRepository.save(processedEvent);
    }

}
