package com.kafka.learning.shipping_service.service;

import com.kafka.learning.events.InventoryReservedEvent;
import com.kafka.learning.events.ShippingCreatedEvent;
import com.kafka.learning.shipping_service.entity.Shipment;
import com.kafka.learning.shipping_service.entity.ShipmentStatus;
import com.kafka.learning.shipping_service.kafka.producer.ShippingProducer;
import com.kafka.learning.shipping_service.repository.ShipmentRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class ShippingService {
    //private final ShippingProducer shippingProducer;

    private final ShipmentRepository shipmentRepository;

    public ShippingService(ShipmentRepository shipmentRepository) {
        this.shipmentRepository = shipmentRepository;
    }

    public void processShipping(InventoryReservedEvent event) {
        System.out.println("--------------------------------");
        System.out.println("Creating Shipment...");
        System.out.println("Order Id : " + event.getOrderId());

        String shipmentId = UUID.randomUUID().toString();

        Shipment shipment = new Shipment(
                shipmentId,
                event.getOrderId(),
                event.getItem(),
                ShipmentStatus.CREATED,
                LocalDateTime.now()
        );

        shipmentRepository.save(shipment);

//        ShippingCreatedEvent shippingCreatedEvent = new ShippingCreatedEvent(
//                event.getOrderId(),
//                shipmentId,
//                "SHIPPED"
//        );

        //shippingProducer.publishShippingCreatedEvent(shippingCreatedEvent);

        System.out.println("Shipment Created");
        System.out.println("--------------------------------");

    }
}
