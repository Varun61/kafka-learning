package com.kafka.learning.payment_service.kafka.producer;

import com.kafka.learning.events.PaymentFailedEvent;
import com.kafka.learning.events.PaymentSuccessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class PaymentProducer {

    private static final String PAYMENT_SUCCESS_TOPIC = "payment-success";
    private static final String PAYMENT_FAILED_TOPIC = "payment-failed";

    private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentSuccessEvent(PaymentSuccessEvent event) throws Exception{

//        kafkaTemplate.send(PAYMENT_SUCCESS_TOPIC, event).get();
//        log.info("PaymentSuccessEvent {} published to Kafka TOPIC {}", event.getPaymentId(), PAYMENT_SUCCESS_TOPIC);

        // Providing a key disables sticky partitioning.
        // Kafka selects the partition by hashing the key.
        //
        // Using a random key distributes different messages across different partitions,
        // increasing the chance that multiple consumer instances can process them in parallel.
        kafkaTemplate.send(PAYMENT_SUCCESS_TOPIC, UUID.randomUUID().toString(), event).get();
        log.info("PaymentSuccessEvent {} published to Kafka TOPIC {}", event.getPaymentId(), PAYMENT_SUCCESS_TOPIC);
    }

    public void publishPaymentFailedEvent(PaymentFailedEvent event) throws Exception{
        kafkaTemplate.send(PAYMENT_FAILED_TOPIC, event).get();
        log.info("PaymentFailedEvent {} published to Kafka TOPIC {}", event.getPaymentId(), PAYMENT_FAILED_TOPIC);
    }

}
