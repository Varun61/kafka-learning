package com.kafka.learning.inventory_service.exception;

public class NotEnoughStockException extends RuntimeException {

    public NotEnoughStockException(String item) {
        super("Not enough stock for item " + item);
    }
}
