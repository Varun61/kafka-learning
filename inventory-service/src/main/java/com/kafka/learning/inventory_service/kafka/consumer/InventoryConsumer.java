package com.kafka.learning.inventory_service.kafka.consumer;

import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.inventory_service.service.InventoryService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;

@Service
@Slf4j
public class InventoryConsumer {

    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "payment-success")
    public void consume(PaymentSuccessEvent event, ConsumerRecord<String, PaymentSuccessEvent> record) {
        log.info("Received PaymentSuccessEvent: {}" , event);

        log.info("Instance: {}", ManagementFactory.getRuntimeMXBean().getName());
        log.info("Partition: {}", record.partition());
        log.info("Offset: {}", record.offset());

        inventoryService.processInventory(event);
    }
}
