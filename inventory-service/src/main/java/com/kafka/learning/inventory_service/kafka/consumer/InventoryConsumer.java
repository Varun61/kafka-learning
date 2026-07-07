package com.kafka.learning.inventory_service.kafka.consumer;

import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.inventory_service.service.InventoryService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {

    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "payments")
    public void consume(PaymentSuccessEvent event) {
        System.out.println("Received PaymentSuccessEvent: " + event);
        inventoryService.processInventory(event);
    }
}
