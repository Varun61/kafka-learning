package com.kafka.learning.payment_service.client;

import com.kafka.learning.payment_service.dto.PaymentRequest;
import com.kafka.learning.payment_service.dto.PaymentResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component //generic object
@RequiredArgsConstructor
public class PaymentGatewayClient {

    private final RestClient restClient;

    public PaymentResponse processPayment(PaymentRequest request) {

        return restClient.post()
                .uri("/payments")
                .body(request)
                .retrieve()
                .body(PaymentResponse.class);
    }
}
