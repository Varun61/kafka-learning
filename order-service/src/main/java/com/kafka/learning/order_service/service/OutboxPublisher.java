package com.kafka.learning.order_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.events.OrderCreatedEvent;
import com.kafka.learning.order_service.entity.OutboxEvent;
import com.kafka.learning.order_service.enums.OutboxStatus;
import com.kafka.learning.order_service.producer.OrderProducer;
import com.kafka.learning.order_service.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository outboxRepository;
    private final OrderProducer orderProducer;
    private final ObjectMapper objectMapper;

    private static final int MAX_RETRIES = 5;

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents =
                outboxRepository.findByStatus(OutboxStatus.PENDING);

        log.info("Found {} pending outbox events to process", pendingEvents.size());

        for(OutboxEvent outboxEvent : pendingEvents) {
            processEvent(outboxEvent);
        }
    }

    private void processEvent(OutboxEvent outboxEvent) {

        OrderCreatedEvent orderCreatedEvent;

        //step 1: deserialize
        try {
            orderCreatedEvent = objectMapper.readValue(
                    outboxEvent.getPayload(),
                    OrderCreatedEvent.class
            );
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize outbox event {}", outboxEvent.getId(), e);

            outboxEvent.setStatus(OutboxStatus.FAILED);
            outboxRepository.save(outboxEvent);

            return;
        }

        //step 2 : publish to Kafka
        try {
            orderProducer.sendOrder(orderCreatedEvent);
        } catch (Exception e) {

            outboxEvent.setRetryCount(outboxEvent.getRetryCount() + 1);

            if(outboxEvent.getRetryCount() > MAX_RETRIES) {

                outboxEvent.setStatus(OutboxStatus.FAILED);
                log.error(
                        "Outbox event {} reached maximum retry limit ({}). Marking as FAILED.",
                        outboxEvent.getId(),
                        MAX_RETRIES,
                        e
                );
            } else {

                log.warn(
                        "Failed to publish outbox event {}. Retry {}/{}.",
                        outboxEvent.getId(),
                        outboxEvent.getRetryCount(),
                        MAX_RETRIES,
                        e
                );
            }

            outboxRepository.save(outboxEvent);

            return;
        }

        //step 3 ; Mark as SENT
        outboxEvent.setStatus(OutboxStatus.SENT);
        outboxRepository.save(outboxEvent);

        log.info("Successfully published outbox event {}", outboxEvent.getId());

    }
}
