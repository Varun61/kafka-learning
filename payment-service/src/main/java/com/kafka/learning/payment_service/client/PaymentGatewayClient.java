package com.kafka.learning.payment_service.client;

import com.kafka.learning.payment_service.dto.PaymentRequest;
import com.kafka.learning.payment_service.dto.PaymentResponse;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component //generic object
@RequiredArgsConstructor
public class PaymentGatewayClient {

    private final RestClient restClient;

    @Retry(name = "paymentGateway") // retries when method receives exception
    public PaymentResponse processPayment(PaymentRequest request) {

        return restClient.post()
                .uri("/payments")
                .body(request)
                .retrieve()
                .body(PaymentResponse.class);
    }
}
