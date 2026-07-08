package com.kafka.learning.payment_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.events.OrderCreatedEvent;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.payment_service.entity.OutboxEvent;
import com.kafka.learning.payment_service.entity.OutboxStatus;
import com.kafka.learning.payment_service.entity.Payment;
import com.kafka.learning.payment_service.entity.PaymentStatus;
import com.kafka.learning.payment_service.kafka.producer.PaymentProducer;
import com.kafka.learning.payment_service.repository.OutboxRepository;
import com.kafka.learning.payment_service.repository.PaymentRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class PaymentService {
    private final PaymentRepository paymentRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public PaymentService(PaymentRepository paymentRepository,
                          OutboxRepository outboxRepository,
                          ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void processPayment(OrderCreatedEvent event) {

        System.out.println("--------------------------------");
        System.out.println("Processing payment...");
        System.out.println("Order Id : " + event.getOrderId());

        String paymentId = UUID.randomUUID().toString();

        Payment payment = new Payment(
                paymentId,
                event.getOrderId(),
                event.getItem(),
                PaymentStatus.PAID
        );

        paymentRepository.save(payment);


        PaymentSuccessEvent paymentSuccessEvent = new PaymentSuccessEvent(
                UUID.randomUUID(),
                event.getOrderId(),
                paymentId,
                event.getItem(),
                event.getQuantity()
        );

        String payload;
        try {
            payload = objectMapper.writeValueAsString(paymentSuccessEvent);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize event", e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(
                UUID.randomUUID(),
                "PaymentSuccessEvent",
                payload,
                OutboxStatus.PENDING,
                LocalDateTime.now(),
                0
        );

        outboxRepository.save(outboxEvent);

        //paymentProducer.publishPaymentSuccessEvent(paymentSuccessEvent);

        System.out.println("Payment Successful");
        System.out.println("--------------------------------");
    }
}
