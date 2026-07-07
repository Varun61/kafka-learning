package com.kafka.learning.inventory_service.kafka.producer;

import com.kafka.learning.events.InventoryReservedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InventoryProducer {
    private static final String TOPIC = "inventory";

    private final KafkaTemplate<String, InventoryReservedEvent> kafkaTemplate;

    public InventoryProducer(KafkaTemplate<String, InventoryReservedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishInventoryReserveEvent(InventoryReservedEvent event) {
        kafkaTemplate.send(TOPIC, event);
        System.out.println("Inventory Reservation Event published: " + event);
    }

}
