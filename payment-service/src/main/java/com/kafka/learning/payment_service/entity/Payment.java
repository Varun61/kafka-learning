package com.kafka.learning.payment_service.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name="payments")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Payment {

    @Id
    private String paymentId;

    private String orderId;
    private String item;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;
}
