package com.kafka.learning.order_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.events.PaymentFailedEvent;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.order_service.dto.OrderRequest;
import com.kafka.learning.order_service.entity.Order;
import com.kafka.learning.order_service.enums.OrderStatus;
import com.kafka.learning.order_service.entity.OutboxEvent;
import com.kafka.learning.order_service.enums.OutboxStatus;
import com.kafka.learning.order_service.repository.OrderRepository;
import com.kafka.learning.order_service.repository.OutboxRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.kafka.learning.events.OrderCreatedEvent;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
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

        UUID orderId = UUID.randomUUID();

        Order order = new Order(
                orderId,
                request.getItem(),
                request.getQuantity(),
                OrderStatus.PAYMENT_PENDING
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

    @Transactional
    public void handlePaymentSuccessEvent(PaymentSuccessEvent paymentSuccessEvent) {

        Order order = orderRepository.findById(paymentSuccessEvent.getOrderId())
                .orElseThrow(() ->
                        new RuntimeException("Order not found: " + paymentSuccessEvent.getOrderId()));

        //make idempotent to save order with already updated status
        if(order.getStatus() != OrderStatus.PAYMENT_PENDING) {
            log.info("Order {} already completed. Ignoring duplicate event.", order.getOrderId());
            return;
        }

        order.setStatus(OrderStatus.PAYMENT_COMPLETED);
        orderRepository.save(order);

        log.info("--------------------------------");
        log.info("Payment completed");
        log.info("Order Id : {} " , order.getOrderId());
        log.info("--------------------------------");

    }

    public void handlePaymentFailedEvent(PaymentFailedEvent paymentFailedEvent) {

        Order order = orderRepository.findById(paymentFailedEvent.getOrderId())
                .orElseThrow(() ->
                        new RuntimeException("Order not found: " + paymentFailedEvent.getOrderId()));

        //make idempotent to save order with already updated status
        if(order.getStatus() != OrderStatus.PAYMENT_PENDING) {
            log.info("Order {} already completed. Ignoring duplicate event.", order.getOrderId());
            return;
        }

        order.setStatus(OrderStatus.PAYMENT_FAILED);

        orderRepository.save(order);

        log.info("--------------------------------");
        log.info("Payment failed");
        log.info("Order Id : {}" , order.getOrderId());
        log.info("--------------------------------");
    }

}