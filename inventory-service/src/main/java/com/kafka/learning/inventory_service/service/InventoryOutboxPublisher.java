package com.kafka.learning.inventory_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.events.InventoryReservedEvent;
import com.kafka.learning.inventory_service.entity.InventoryOutboxEvent;
import com.kafka.learning.inventory_service.entity.InventoryOutboxStatus;
import com.kafka.learning.inventory_service.kafka.producer.InventoryProducer;
import com.kafka.learning.inventory_service.repository.InventoryOutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class InventoryOutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(InventoryOutboxPublisher.class);

    private final InventoryOutboxRepository inventoryOutboxRepository;
    private final InventoryProducer inventoryProducer;
    private final ObjectMapper objectMapper;

    private static final int MAX_RETRIES = 5;

    InventoryOutboxPublisher( InventoryOutboxRepository inventoryOutboxRepository,
                              InventoryProducer inventoryProducer,
                              ObjectMapper objectMapper) {
        this.inventoryOutboxRepository = inventoryOutboxRepository;
        this.inventoryProducer = inventoryProducer;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedRate = 5000)
    public void publishPendingEvents() {
        List<InventoryOutboxEvent> pendingEvents =
                inventoryOutboxRepository.findByStatus(InventoryOutboxStatus.PENDING);

        log.info("Found {} pending outbox events to process", pendingEvents.size());

        for(InventoryOutboxEvent outboxEvent : pendingEvents) {
            processEvent(outboxEvent);
        }
    }

    private void processEvent(InventoryOutboxEvent outboxEvent) {

        InventoryReservedEvent inventoryReservedEvent;

        //step 1: deserialize
        try{
            inventoryReservedEvent = objectMapper.readValue(
                    outboxEvent.getPayload(),
                    InventoryReservedEvent.class
            );
        } catch(JsonProcessingException e){
            log.error("Failed to deserialize outbox event {}", outboxEvent.getId(), e);

            outboxEvent.setStatus(InventoryOutboxStatus.FAILED);
            inventoryOutboxRepository.save(outboxEvent);

            return;
        }

        //step 1: publish to Kafka
        try{
            inventoryProducer.publishInventoryReserveEvent(inventoryReservedEvent);
        } catch(Exception e){

            outboxEvent.setRetryCount(outboxEvent.getRetryCount() + 1);

            if(outboxEvent.getRetryCount() >  MAX_RETRIES) {
                outboxEvent.setStatus(InventoryOutboxStatus.FAILED);
                log.error(
                        "Outbox event {} reached maximum retry limit ({}). Marking as FAILED.",
                        outboxEvent.getId(),
                        MAX_RETRIES,
                        e
                );
            }
            else {
                log.warn(
                        "Failed to publish outbox event {}. Retry {}/{}.",
                        outboxEvent.getId(),
                        outboxEvent.getRetryCount(),
                        MAX_RETRIES,
                        e
                );
            }

            inventoryOutboxRepository.save(outboxEvent);

            return;
        }

        //step 3; mark as SENT
        outboxEvent.setStatus(InventoryOutboxStatus.SENT);
        inventoryOutboxRepository.save(outboxEvent);

        log.info("Successfully published outbox event {}", outboxEvent.getId());
    }
}
