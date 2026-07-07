package com.kafka.learning.payment_service.kafka.producer;

import com.kafka.learning.events.PaymentSuccessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentProducer {

    private static final String TOPIC = "payments";

    private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);

    private final KafkaTemplate<String, PaymentSuccessEvent> kafkaTemplate;

    public PaymentProducer(KafkaTemplate<String, PaymentSuccessEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentSuccessEvent(PaymentSuccessEvent event) throws Exception{
        kafkaTemplate.send(TOPIC, event).get();
        log.info("Payment {} published to Kafka TOPIC {}", event.getPaymentId(), TOPIC);
    }

}
