package com.kafka.learning.order_service.consumer;

import com.kafka.learning.events.PaymentFailedEvent;
import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.order_service.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PaymentEventConsumer {

    private final OrderService orderService;

    @KafkaListener(
            topics = "payment-success",
            groupId = "order-group"
    )
    public void handlePaymentSuccessEvent(PaymentSuccessEvent paymentSuccessEvent) {
        orderService.handlePaymentSuccessEvent(paymentSuccessEvent);
    }

    @KafkaListener(
            topics = "payment-failed",
            groupId = "order-group"
    )
    public void handlePaymentFailedEvent(PaymentFailedEvent paymentSuccessEvent) {
        orderService.handlePaymentFailedEvent(paymentSuccessEvent);
    }
}
