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

@RestController
@RequestMapping("/payments")
public class PaymentGatewayController {

    @PostMapping
    public ResponseEntity<PaymentResponse> processPayment(@RequestBody PaymentRequest request) {

        PaymentResponse response = new PaymentResponse(
                UUID.randomUUID().toString(),
                PaymentStatus.SUCCESS,
                "Payment processed successfully."
        );

        System.out.println(response.toString());

        return ResponseEntity.status(HttpStatus.OK).body(response);
    }
}
