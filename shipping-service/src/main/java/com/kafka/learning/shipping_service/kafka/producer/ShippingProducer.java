package com.kafka.learning.shipping_service.kafka.producer;

import com.kafka.learning.events.ShippingCreatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ShippingProducer {

    private static final String TOPIC = "shipping";

    private final KafkaTemplate<String, ShippingCreatedEvent> kafkaTemplate;

    public ShippingProducer(KafkaTemplate<String, ShippingCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishShippingCreatedEvent(ShippingCreatedEvent event) {
        this.kafkaTemplate.send(TOPIC, event);
        System.out.println("Shipping Created Event Published " + event);
    }

}
