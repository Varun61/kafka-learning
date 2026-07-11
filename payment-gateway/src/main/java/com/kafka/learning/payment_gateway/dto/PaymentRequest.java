package com.kafka.learning.payment_gateway.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRequest {

    private String merchantReference; //OrderID
    private double amount;
    private String currency;
}
