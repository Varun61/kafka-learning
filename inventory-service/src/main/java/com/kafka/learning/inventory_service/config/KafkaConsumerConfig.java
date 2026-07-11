package com.kafka.learning.inventory_service.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = // decides what to do on failure, here puts message in Dead letter topic (dlt)
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) ->
                                new TopicPartition(
                                        "payments-dlt",
                                        record.partition()
                                )
                );

        FixedBackOff fixedBackOff = new FixedBackOff(1000L,3); //1000L == 1000ms // 3 retries after initial attempt

        return new DefaultErrorHandler(recoverer, fixedBackOff); // once retries are exhausted, calls recoverer
    }
}
