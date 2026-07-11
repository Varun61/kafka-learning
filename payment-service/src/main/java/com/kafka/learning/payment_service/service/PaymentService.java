package com.kafka.learning.payment_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.events.OrderCreatedEvent;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.payment_service.client.PaymentGatewayClient;
import com.kafka.learning.payment_service.dto.PaymentRequest;
import com.kafka.learning.payment_service.dto.PaymentResponse;
import com.kafka.learning.payment_service.entity.OutboxEvent;
import com.kafka.learning.payment_service.entity.OutboxStatus;
import com.kafka.learning.payment_service.entity.Payment;
import com.kafka.learning.payment_service.enums.PaymentStatus;
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

        if(response.getStatus() == PaymentStatus.SUCCESS) {

                Payment payment = new Payment(
                        request.getPaymentId(),
                        event.getOrderId(),
                        event.getItem(),
                        response.getStatus()
                );

                paymentRepository.save(payment);


                PaymentSuccessEvent paymentSuccessEvent = new PaymentSuccessEvent(
                        UUID.randomUUID(),
                        event.getOrderId(),
                        request.getPaymentId(),
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
        else {
            throw new RuntimeException("Payment failed");
        }
    }
}
