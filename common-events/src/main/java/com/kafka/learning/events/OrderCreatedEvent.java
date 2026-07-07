package com.kafka.learning.events;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderCreatedEvent {
    private String orderId;
    private String item;
    private int quantity;
}
