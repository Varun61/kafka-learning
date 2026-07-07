package com.kafka.learning.shipping_service.kafka.consumer;

import com.kafka.learning.events.InventoryReservedEvent;
import com.kafka.learning.shipping_service.service.ShippingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ShippingConsumer {
    private ShippingService shippingService;

    public ShippingConsumer(ShippingService shippingService) {
        this.shippingService = shippingService;
    }

    @KafkaListener(topics = "inventory")
    public void consume(InventoryReservedEvent event) {
        System.out.println("Received Inventory Reserved Event: " + event);
        shippingService.processShipping(event);
    }
}
