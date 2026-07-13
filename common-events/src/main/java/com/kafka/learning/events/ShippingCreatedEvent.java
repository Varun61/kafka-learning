package com.kafka.learning.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShippingCreatedEvent {
    private UUID orderId;
    private UUID shippingId;
    private String status;
}
