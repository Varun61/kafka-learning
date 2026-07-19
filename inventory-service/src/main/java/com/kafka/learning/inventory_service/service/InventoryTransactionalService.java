package com.kafka.learning.inventory_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.events.InventoryReservedEvent;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.inventory_service.entity.Inventory;
import com.kafka.learning.inventory_service.entity.InventoryOutboxEvent;
import com.kafka.learning.inventory_service.entity.InventoryOutboxStatus;
import com.kafka.learning.inventory_service.exception.NotEnoughStockException;
import com.kafka.learning.inventory_service.idempotency.IdempotencyService;
import com.kafka.learning.inventory_service.repository.InventoryOutboxRepository;
import com.kafka.learning.inventory_service.repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class InventoryTransactionalService {

    private final InventoryRepository inventoryRepository;
    private final InventoryOutboxRepository inventoryOutboxRepository;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @Transactional
    public void reserveInventory(PaymentSuccessEvent event) {
        log.info("--------------------------------------------------");
        log.info("Processing inventory for Order ID: {}", event.getOrderId());

        Inventory inventory = inventoryRepository.findById(event.getItem()).orElseThrow(() ->
                new RuntimeException("Item not found: " + event.getItem()));


        // pessimistic locking - used when high contention
//        Inventory inventory = inventoryRepository
//                .findByItemForUpdate(event.getItem())
//                .orElseThrow(() ->
//                        new RuntimeException("Item not found: " + event.getItem()));

        log.info("Read quantity = {}", inventory.getAvailableQuantity());

        // Adding sleep to replicate race conditions.
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if(inventory.getAvailableQuantity() < event.getQuantity() ) {
            throw new NotEnoughStockException(event.getItem());
        }

        inventory.setAvailableQuantity(inventory.getAvailableQuantity() - event.getQuantity());

        //try {
            inventoryRepository.save(inventory);
            //inventoryRepository.flush(); // commits the db transaction but usually all transactions are commited at end.
            // this is the example of optimistic locking, verifies data as changed using version at time of commiting.
        //} catch (Exception e) {
        //    log.error("Flush failed", e);
        //    throw e;
        //}

        log.info("Inventory reserved successfully.");
        log.info("Item               : {}", inventory.getItem());
        log.info("Remaining Quantity : {}", inventory.getAvailableQuantity());
        log.info("--------------------------------------------------");

        InventoryReservedEvent inventoryReservedEvent =
                new InventoryReservedEvent(
                        event.getOrderId(),
                        UUID.randomUUID(),
                        event.getItem()
                );
        String payload;
        try {
            payload = objectMapper.writeValueAsString(inventoryReservedEvent);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }

        InventoryOutboxEvent inventoryOutboxEvent =
                new InventoryOutboxEvent(
                        UUID.randomUUID(),
                        "InventoryReservedEvent",
                        payload,
                        InventoryOutboxStatus.PENDING,
                        LocalDateTime.now(),
                        0
                );

        inventoryOutboxRepository.save(inventoryOutboxEvent);

        idempotencyService.markProcessed(event.getEventId());
    }
}
