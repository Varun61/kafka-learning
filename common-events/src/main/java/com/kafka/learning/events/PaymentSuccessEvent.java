package com.kafka.learning.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentSuccessEvent {
    private UUID eventID;
    private String orderId;
    private String paymentId;
    private String item;
}
