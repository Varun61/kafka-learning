package com.kafka.learning.order_service.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.kafka.learning.events.OrderCreatedEvent;

@Service
public class OrderProducer {
    private static final String TOPIC = "orders";

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(OrderCreatedEvent event) throws Exception {

        kafkaTemplate.send(TOPIC, event).get(); // returns ACK is message is sent successfully, throw exception if fails

        log.info("Order {} published to Kafka topic {}", event.getOrderId(), TOPIC);
    }
}
