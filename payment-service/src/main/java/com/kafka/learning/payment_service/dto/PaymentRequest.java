package com.kafka.learning.payment_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRequest {

    private String marchantReference; //OrderID - so if gateway uses webhook, we can recognize the order the payment is for.
    private double amount;
    private String currency;
}
