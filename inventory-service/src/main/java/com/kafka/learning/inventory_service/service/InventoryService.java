package com.kafka.learning.inventory_service.service;

import com.kafka.learning.events.InventoryReservedEvent;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.inventory_service.kafka.producer.InventoryProducer;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class InventoryService {
    private final InventoryProducer inventoryProducer;

    public InventoryService(InventoryProducer inventoryProducer) {
        this.inventoryProducer = inventoryProducer;
    }

    public void processInventory(PaymentSuccessEvent event) {

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

    }
}
