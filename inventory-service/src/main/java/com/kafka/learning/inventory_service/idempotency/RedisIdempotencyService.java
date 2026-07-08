package com.kafka.learning.inventory_service.idempotency;

import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

@Service
//@Primary // Tells spring to use only this interface implem
public class RedisIdempotencyService implements IdempotencyService {

    private static final Duration TTL = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;

    public RedisIdempotencyService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean isDuplicate(UUID eventId){
        System.out.println("Checking Redis for event id: " + eventId);

        return Boolean.TRUE.equals(
                redisTemplate.hasKey(getKey(eventId))
        );
    }

    @Override
    public void markProcessed(UUID eventId) {
        System.out.println("Saving event to Redis: " + eventId);
        redisTemplate.opsForValue().set(
                getKey(eventId),
                "processed",
                TTL
        );
    }

    private String getKey(UUID eventId){
        return "processed-event:" + eventId;
    }
}
