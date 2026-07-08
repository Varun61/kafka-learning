package com.kafka.learning.inventory_service.kafka.producer;

import com.kafka.learning.events.InventoryReservedEvent;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InventoryProducer {
    private static final String TOPIC = "inventory";

    private static final Logger log = LoggerFactory.getLogger(InventoryProducer.class);

    private final KafkaTemplate<String, InventoryReservedEvent> kafkaTemplate;

    public InventoryProducer(KafkaTemplate<String, InventoryReservedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishInventoryReserveEvent(InventoryReservedEvent event) throws Exception {
        kafkaTemplate.send(TOPIC, event).get();
        log.info("Inventory event {} sent to Kafka Topic {}", event.getInventoryId(), TOPIC);
    }

}
