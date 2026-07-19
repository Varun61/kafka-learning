package com.kafka.learning.inventory_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.inventory_service.entity.FailedMessage;
import com.kafka.learning.inventory_service.entity.FailedStatus;
import com.kafka.learning.inventory_service.repository.FailedMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplayService {

    private final FailedMessageRepository failedMessageRepository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, PaymentSuccessEvent> kafkaTemplate;

    public void replayFailedMessage() {

        List<FailedMessage> failedMessages =
                failedMessageRepository.findByStatus(FailedStatus.FAILED);

        log.info("Found {} failed messages to replay.", failedMessages.size());

        for (FailedMessage failedMessage : failedMessages) {

            try {
                PaymentSuccessEvent event =
                        objectMapper.readValue(
                                failedMessage.getPayload(),
                                PaymentSuccessEvent.class
                        );

                kafkaTemplate.send("payment-success", event).get();

                failedMessage.setStatus(FailedStatus.REPLAYED);

                failedMessageRepository.save(failedMessage);

                log.info("Successfully replayed Event ID {} ", failedMessage.getEventId());

            } catch(Exception e) {

                log.error("Replay Failed for Event ID {} ",  failedMessage.getEventId(), e);
            }
        }
    }
}
