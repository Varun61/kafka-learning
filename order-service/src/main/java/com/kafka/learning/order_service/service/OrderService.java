package com.kafka.learning.order_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.order_service.dto.OrderRequest;
import com.kafka.learning.order_service.entity.Order;
import com.kafka.learning.order_service.entity.OutboxEvent;
import com.kafka.learning.order_service.entity.OutboxStatus;
import com.kafka.learning.order_service.repository.OrderRepository;
import com.kafka.learning.order_service.repository.OutboxRepository;
import org.springframework.stereotype.Service;
import com.kafka.learning.events.OrderCreatedEvent;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public String createOrder(OrderRequest request) {

        String orderId = UUID.randomUUID().toString();

        Order order = new Order(
                orderId,
                request.getItem(),
                request.getQuantity(),
                "CREATED"
        );

        orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
                                        UUID.randomUUID(),
                                        orderId,
                                        request.getItem(),
                                        request.getQuantity()
        );

        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(
                UUID.randomUUID(),
                "OrderCreatedEvent",
                payload,
                OutboxStatus.PENDING,
                LocalDateTime.now(),
                0
        );

        outboxRepository.save(outboxEvent);

        return "Order Accepted";
    }
}