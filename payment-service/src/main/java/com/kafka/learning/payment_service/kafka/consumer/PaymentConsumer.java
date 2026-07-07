package com.kafka.learning.payment_service.kafka.consumer;

import com.kafka.learning.events.OrderCreatedEvent;
import com.kafka.learning.payment_service.service.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class PaymentConsumer {

    private final PaymentService paymentService;

    public PaymentConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "orders")
    public void consume(OrderCreatedEvent event) {

        System.out.println("Received Order Created Event");

        paymentService.processPayment(event);
    }
}
