package com.kafka.learning.inventory_service.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.inventory_service.entity.FailedMessage;
import com.kafka.learning.inventory_service.entity.FailedStatus;
import com.kafka.learning.inventory_service.repository.FailedMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class DLQConsumer {

    Logger log = LoggerFactory.getLogger(DLQConsumer.class);
    private final FailedMessageRepository failedMessageRepository;
    private final ObjectMapper objectMapper;

    public DLQConsumer(FailedMessageRepository failedMessageRepository,
                       ObjectMapper objectMapper) {
        this.failedMessageRepository = failedMessageRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "payment-success-dlt", groupId = "inventory-dlt-group")
    public void consume(PaymentSuccessEvent event) throws JsonProcessingException {

        log.info("==================================================");
        log.info("Received message from DLQ");
        log.info("Order ID : {}", event.getOrderId());
        log.info("Event ID : {}", event.getEventId());
        log.info("==================================================");

        String payload = objectMapper.writeValueAsString(event);

        FailedMessage failedMessage =
                new FailedMessage(
                        UUID.randomUUID(),
                        event.getEventId(),
                        payload,
                        "Not enough stock",
                        LocalDateTime.now(),
                        FailedStatus.FAILED
        );

        failedMessageRepository.save(failedMessage);

        log.info("Failed message saved successfully.");
    }
}
