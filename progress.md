# 🚀 Kafka Learning Project Progress

> This document tracks the implementation progress of my Kafka Microservices Learning Project.
> The goal is not only to build a distributed system, but to understand *why* every pattern exists and what problem it solves.

---

# 🎯 Project Goal

Build a production-inspired event-driven microservices architecture using Spring Boot, Kafka, PostgreSQL and Redis while learning:

- Event Driven Architecture
- Distributed Systems
- Resiliency Patterns
- Scalability
- Data Consistency
- System Design

---

# ✅ Completed

## Phase 1 — Kafka Fundamentals

- [x] Kafka Producer
- [x] Kafka Consumer
- [x] Kafka Topics
- [x] Kafka Partitions
- [x] Consumer Groups
- [x] JSON Serialization / Deserialization
- [x] Kafka UI
- [x] Multiple Microservices

---

## Order Service

- [x] Publish OrderCreatedEvent
- [x] Kafka Producer Configuration
- [x] PostgreSQL Integration

---

## Inventory Service

- [x] Consume OrderCreatedEvent
- [x] Inventory Validation
- [x] Inventory Deduction
- [x] Inventory Repository

---

## Payment Service

- [x] Consume OrderCreatedEvent
- [x] Payment Processing
- [x] Payment Persistence
- [x] Payment Gateway Integration
- [x] Payment Transaction Storage

---

## Shipping Service

- [x] Consume PaymentSuccessEvent
- [x] Shipping Persistence

---

# Reliability Patterns

## Outbox Pattern

- [x] Outbox Table
- [x] Transactional Save
- [x] Outbox Publisher
- [x] Scheduled Publishing

### Learned

- Why Dual Writes are dangerous
- How Outbox guarantees consistency

---

## Dead Letter Queue (DLQ)

- [x] Kafka DLQ
- [x] DeadLetterPublishingRecoverer
- [x] DLQ Consumer
- [x] Failed Event Persistence

### Learned

- Difference between Retry and DLQ
- Why failed messages should never disappear
- Manual recovery strategies

---

## Idempotency

- [x] Processed Events Table
- [x] Duplicate Event Detection

### Learned

- At-Least-Once Delivery
- Why duplicate messages happen
- Safe event processing

---

## Payment Gateway

- [x] Mock Payment Gateway
- [x] Random Success
- [x] Business Failure Simulation
- [x] Infrastructure Failure Simulation

### Learned

Difference between:

- Business Failure
- Infrastructure Failure

---

## Retry

### Kafka Retry

- [x] Kafka Listener Retry
- [x] Retry Topics

### HTTP Retry

- [x] Resilience4j Retry
- [x] Exponential Backoff

### Learned

Difference between:

- HTTP Retry
- Kafka Retry

and why both are needed.

---

## Circuit Breaker

- [x] Resilience4j Circuit Breaker
- [x] Closed State
- [x] Open State
- [x] Half Open State

### Tested

- Gateway crash
- Automatic recovery
- Half-open behaviour

### Learned

How Circuit Breaker protects downstream services.

---

# Databases

- [x] PostgreSQL
- [x] Multiple Databases
- [x] Spring Data JPA

---

# Infrastructure

- [x] Kafka
- [x] PostgreSQL
- [x] Redis
- [x] Kafka UI
- [x] Podman Compose

---

# 📚 Biggest Learnings So Far

- Kafka send acknowledgement only confirms the broker received the message.
- It does NOT mean another service processed it.
- Retry and DLQ solve different problems.
- Outbox Pattern removes dual write inconsistency.
- Idempotency is mandatory in distributed systems.
- Business failures should usually not be retried.
- Infrastructure failures should be retried.
- Circuit Breaker protects unhealthy downstream services.
- HTTP Retry should happen before Kafka Retry.
- Logs are often the best debugging tool for distributed systems.

---

# 🚧 Currently Working On

- [ ] Payment Failed Event
- [ ] Complete Payment Flow

---

# 🗺️ Future Roadmap

## Reliability

- [ ] Payment Failure Events
- [ ] Saga Pattern
- [ ] Compensation Transactions

---

## API Layer

- [ ] Spring Cloud Gateway
- [ ] API Gateway
- [ ] Global Exception Handling
- [ ] Authentication
- [ ] Authorization

---

## Scalability

- [ ] Multiple Producer Instances
- [ ] Multiple Consumer Instances
- [ ] Consumer Rebalancing
- [ ] Kafka Partition Strategies
- [ ] Sticky Partitioning
- [ ] Producer Tuning
- [ ] Consumer Tuning

---

## Concurrency

- [ ] Optimistic Locking
- [ ] Pessimistic Locking
- [ ] Overselling Prevention
- [ ] Concurrent Inventory Updates

---

## Infrastructure

- [ ] Load Balancer
- [ ] Reverse Proxy
- [ ] Rate Limiter
- [ ] Redis Improvements

---

## Monitoring

- [ ] Metrics
- [ ] Health Checks
- [ ] Observability
- [ ] Logging Improvements

---

# 📖 Final Goal

Build a production-inspired microservices architecture demonstrating:

- Event Driven Architecture
- Reliable Messaging
- Distributed Transactions
- Fault Tolerance
- High Availability
- Scalability
- Clean Architecture
- Production Ready Practices

---

# 💡 Personal Notes

This project is intentionally built step-by-step.

The goal is **not** to finish quickly.

The goal is to deeply understand every pattern by implementing it, breaking it, debugging it, and learning why it exists before moving to the next one.
