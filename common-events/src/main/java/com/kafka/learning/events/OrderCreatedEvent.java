package com.kafka.learning.events;

import lombok.*;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderCreatedEvent {
    private UUID eventId;
    private String orderId;
    private String item;
    private int quantity;
}
