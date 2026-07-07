package com.kafka.learning.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ShippingCreatedEvent {
    private String orderId;
    private String shippingId;
    private String status;
}
