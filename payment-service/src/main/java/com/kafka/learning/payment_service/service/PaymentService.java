package com.kafka.learning.payment_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.events.OrderCreatedEvent;
import com.kafka.learning.events.PaymentFailedEvent;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.payment_service.client.PaymentGatewayClient;
import com.kafka.learning.payment_service.dto.PaymentRequest;
import com.kafka.learning.payment_service.dto.PaymentResponse;
import com.kafka.learning.payment_service.entity.OutboxEvent;
import com.kafka.learning.payment_service.entity.OutboxStatus;
import com.kafka.learning.payment_service.entity.Payment;
import com.kafka.learning.payment_service.repository.OutboxRepository;
import com.kafka.learning.payment_service.repository.PaymentRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final PaymentGatewayClient paymentGatewayClient;

    public PaymentService(PaymentRepository paymentRepository,
                          OutboxRepository outboxRepository,
                          ObjectMapper objectMapper,
                          PaymentGatewayClient paymentGatewayClient) {
        this.paymentRepository = paymentRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.paymentGatewayClient = paymentGatewayClient;
    }

    @Transactional
    @CircuitBreaker(name = "paymentGateway", fallbackMethod = "paymentGatewayFallback")
    public void processPayment(OrderCreatedEvent event) {

        System.out.println("--------------------------------");
        System.out.println("Processing payment...");
        System.out.println("Order Id : " + event.getOrderId());


        PaymentRequest request = new PaymentRequest(
                UUID.randomUUID().toString(),
                event.getQuantity() * 100.0,
                "INR"
        );

        PaymentResponse response =
                paymentGatewayClient.processPayment(request);

        switch(response.getStatus()) {

            case SUCCESS -> handleSuccessfulPayment(event, response);

            case FAILED -> handleFailedPayment(event, response);

            default -> throw new RuntimeException("Unknown payment status: " + response.getStatus());
        }
    }

    private void handleSuccessfulPayment(OrderCreatedEvent event, PaymentResponse response) {

        Payment payment = savePayment(event, response);

        PaymentSuccessEvent paymentSuccessEvent = new PaymentSuccessEvent(
                UUID.randomUUID(),
                event.getOrderId(),
                payment.getPaymentId(),
                event.getItem(),
                event.getQuantity()
        );

        String payload = serialize(paymentSuccessEvent);

        OutboxEvent outboxEvent = new OutboxEvent(
                UUID.randomUUID(),
                "PaymentSuccessEvent",
                payload,
                OutboxStatus.PENDING,
                LocalDateTime.now(),
                0
        );

        outboxRepository.save(outboxEvent);

        System.out.println("Payment Successful");
        System.out.println("--------------------------------");
    }

    private void handleFailedPayment(OrderCreatedEvent event, PaymentResponse response) {

        Payment payment = savePayment(event, response);

        PaymentFailedEvent paymentFailedEvent = new PaymentFailedEvent(
                UUID.randomUUID(),
                event.getOrderId(),
                payment.getPaymentId(),
                "Payment Failed"
        );

        String payload;

        try {
            payload = objectMapper.writeValueAsString(paymentFailedEvent);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(
                UUID.randomUUID(),
                "PaymentFailedEvent",
                payload,
                OutboxStatus.PENDING,
                LocalDateTime.now(),
                0
        );

        outboxRepository.save(outboxEvent);

        System.out.println("--------------------------------");
        System.out.println("Payment Failed");
        System.out.println("Order Id : " + event.getOrderId());
        System.out.println("--------------------------------");
    }

    private Payment savePayment(OrderCreatedEvent event, PaymentResponse response) {

        Payment payment = new Payment(
                UUID.randomUUID(),
                response.getTransactionId(),
                event.getOrderId(),
                event.getItem(),
                response.getStatus()
        );

        return paymentRepository.save(payment);
    }

    private String serialize(Object object) {

        try {
            return objectMapper.writeValueAsString(object);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to serialize", e);
        }
    }

    private void paymentGatewayFallback(
            OrderCreatedEvent event,
            Exception exception) {

        System.out.println("--------------------------------");
        System.out.println("Circuit Breaker Fallback");
        System.out.println("Order Id : " + event.getOrderId());
        System.out.println("Reason   : " + exception.getMessage());
        System.out.println("--------------------------------");

        throw new RuntimeException("Payment Gateway currently unavailable.");
    }
}
