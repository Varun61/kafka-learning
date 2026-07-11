package com.kafka.learning.payment_service.dto;

import com.kafka.learning.payment_service.enums.PaymentStatus;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentResponse {

    private String transactionId;
    private PaymentStatus status;
    private String message;
}
