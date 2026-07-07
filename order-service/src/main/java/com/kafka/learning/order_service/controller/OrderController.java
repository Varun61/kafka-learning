package com.kafka.learning.order_service.controller;

import com.kafka.learning.order_service.dto.OrderRequest;
import com.kafka.learning.order_service.service.OrderService;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("/hello")
    public String hello() {
        return "Kafka Journey Begins 🚀";
    }

    @PostMapping
    public String createOrder(@RequestBody OrderRequest orderRequest) {
        return orderService.createOrder(orderRequest);
    }
}
