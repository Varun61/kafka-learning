package com.kafka.learning.payment_service.kafka.producer;

import com.kafka.learning.events.PaymentSuccessEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentProducer {

    private static final String TOPIC = "payments";

    private final KafkaTemplate<String, PaymentSuccessEvent> kafkaTemplate;

    public PaymentProducer(KafkaTemplate<String, PaymentSuccessEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentSuccessEvent(PaymentSuccessEvent event) {
        kafkaTemplate.send(TOPIC, event);
        System.out.println("Payment Success Event Published: " + event);
    }

}
