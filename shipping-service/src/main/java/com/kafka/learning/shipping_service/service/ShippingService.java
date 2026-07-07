package com.kafka.learning.shipping_service.service;

import com.kafka.learning.events.InventoryReservedEvent;
import com.kafka.learning.events.ShippingCreatedEvent;
import com.kafka.learning.shipping_service.kafka.producer.ShippingProducer;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ShippingService {
    private final ShippingProducer shippingProducer;

    public ShippingService(ShippingProducer shippingProducer) {
        this.shippingProducer = shippingProducer;
    }

    public void processShipping(InventoryReservedEvent event) {
        System.out.println("--------------------------------");
        System.out.println("Creating Shipment...");
        System.out.println("Order Id : " + event.getOrderId());

        String shipmentId = UUID.randomUUID().toString();

        ShippingCreatedEvent shippingCreatedEvent = new ShippingCreatedEvent(
                event.getOrderId(),
                shipmentId,
                "SHIPPED"
        );

        shippingProducer.publishShippingCreatedEvent(shippingCreatedEvent);

        System.out.println("Shipment Created");
        System.out.println("--------------------------------");

    }
}
