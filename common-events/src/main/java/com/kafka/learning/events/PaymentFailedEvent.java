package com.kafka.learning.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentFailedEvent {

    private UUID eventId;
    private UUID orderId;
    private UUID paymentId;
    private String reason;
}
