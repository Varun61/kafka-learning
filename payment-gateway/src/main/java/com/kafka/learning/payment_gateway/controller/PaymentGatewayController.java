package com.kafka.learning.payment_gateway.controller;

import com.kafka.learning.payment_gateway.dto.PaymentRequest;
import com.kafka.learning.payment_gateway.dto.PaymentResponse;
import com.kafka.learning.payment_gateway.enums.PaymentStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping("/payments")
public class PaymentGatewayController {

    @PostMapping
    public ResponseEntity<PaymentResponse> processPayment(@RequestBody PaymentRequest request) {

        //Adding randomness to handle failure scenario in payment service
        int random = ThreadLocalRandom.current().nextInt(100);

        // 70% Success
        if (random < 70) {

            PaymentResponse response = new PaymentResponse(
                    UUID.randomUUID().toString(),
                    PaymentStatus.SUCCESS,
                    "Payment processed successfully."
            );

            System.out.println("SUCCESS -> " + response);

            return ResponseEntity.ok(response);
        }

        // 20% Business Failure (card declined, insufficient funds, etc.)
        if(random < 90) {

            PaymentResponse response = new PaymentResponse(
                    UUID.randomUUID().toString(),
                    PaymentStatus.FAILED,
                    "Payment Failed"
            );

            System.out.println("FAILED -> " + response);

            return ResponseEntity.ok(response);
        }

        // 10% gateway crash
        System.out.println("Gateway Crashed");

        throw new RuntimeException("Payment Gateway Internal Error");
    }
}
