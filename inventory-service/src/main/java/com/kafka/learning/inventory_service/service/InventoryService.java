package com.kafka.learning.inventory_service.service;

import com.kafka.learning.events.InventoryReservedEvent;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.inventory_service.idempotency.IdempotencyService;
import com.kafka.learning.inventory_service.kafka.producer.InventoryProducer;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class InventoryService {
    private final InventoryProducer inventoryProducer;
    private final IdempotencyService idempotencyService;

    public InventoryService(InventoryProducer inventoryProducer,
                            IdempotencyService idempotencyService) {
        this.inventoryProducer = inventoryProducer;
        this.idempotencyService = idempotencyService;
    }

    public void processInventory(PaymentSuccessEvent event) {

        if(idempotencyService.isDuplicate(event.getEventID())) {
            System.out.println("Duplicate event detected. Ignoring event: "  + event.getEventID());
            return;
        }

        System.out.println("--------------------------------");
        System.out.println("Reserving Inventory...");
        System.out.println("Order Id : " + event.getOrderId());

        String inventoryId = UUID.randomUUID().toString();

        InventoryReservedEvent inventoryReservedEvent = new InventoryReservedEvent(
                event.getOrderId(),
                inventoryId,
                event.getItem()
        );

        inventoryProducer.publishInventoryReserveEvent(inventoryReservedEvent);

        System.out.println("Reservation Successful");
        System.out.println("--------------------------------");

        idempotencyService.markProcessed(event.getEventID());

    }
}
