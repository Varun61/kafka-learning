package com.kafka.learning.inventory_service.service;

import com.kafka.learning.events.PaymentSuccessEvent;
import com.kafka.learning.inventory_service.idempotency.IdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private final IdempotencyService idempotencyService;
    private final InventoryTransactionalService inventoryTransactionalService;

    public InventoryService(IdempotencyService idempotencyService,
                            InventoryTransactionalService inventoryTransactionalService) {
        this.idempotencyService = idempotencyService;
        this.inventoryTransactionalService = inventoryTransactionalService;
    }

    public void processInventory(PaymentSuccessEvent event) {

        if(idempotencyService.isDuplicate(event.getEventId())) {
            log.warn("Duplicate event detected. Ignoring Event ID: {}", event.getEventId());
            return;
        }

        int attempts = 0;

        while (true) {

            try {
                inventoryTransactionalService.reserveInventory(event);
                return;
            } catch (ObjectOptimisticLockingFailureException e) {

                attempts++;

                log.warn("Optimistic locking failed. Attempt {}", attempts);

                if(attempts >= 3) {
                    throw e;
                }

                try  {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
