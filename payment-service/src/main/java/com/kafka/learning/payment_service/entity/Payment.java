package com.kafka.learning.payment_service.entity;

import com.kafka.learning.payment_service.enums.PaymentStatus;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Entity
@Table(name="payments")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Payment {

    @Id
    private UUID paymentId;

    private String gateway_transactionId;

    private UUID orderId;
    private String item;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;
}
