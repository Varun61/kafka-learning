package com.kafka.learning.order_service.entity;

import com.kafka.learning.order_service.enums.OrderStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Entity
@Table(name="orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    @Id
    private UUID orderId;

    private String item;
    private int quantity;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;
}
