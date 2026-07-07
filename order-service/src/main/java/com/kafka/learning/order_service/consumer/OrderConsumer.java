//package com.kafka.learning.order_service.consumer;
//
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//import com.kafka.learning.events.OrderCreatedEvent;
//
//@Service
//public class OrderConsumer {
//    @KafkaListener(topics = "orders", groupId = "order-group")
//    public void consume(OrderCreatedEvent message) {
//        System.out.println("Order Received : " + message.getOrderId());
//        System.out.println("Order Status : " + message.getStatus());
//    }
//}
