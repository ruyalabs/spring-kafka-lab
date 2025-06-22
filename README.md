# Spring Kafka Lab - Payment Processing System

## Overview

This Spring Boot application demonstrates a robust payment processing system using Apache Kafka for message-driven architecture. The system processes payment requests asynchronously, handles various error scenarios with sophisticated retry mechanisms, and provides comprehensive structured logging for monitoring and debugging.

## Architecture

The application follows a microservices pattern with the following key components:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Kafka Topic   │    │  Payment Request │    │   Kafka Topic   │
│ payment-request │───▶│    Consumer      │───▶│ payment-response│
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Balance Check   │
                    │     Client       │
                    └──────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Payment Execution│
                    │     Client       │
                    └──────────────────┘
```

### Core Components

1. **PaymentRequestConsumer** - Main message consumer that orchestrates payment processing
2. **BalanceCheckClient** - Simulates external balance verification service
3. **PaymentExecutionClient** - Simulates external payment execution service
4. **PaymentResponseProducer** - Sends success/error responses to response topic
5. **PaymentRequestErrorHandler** - Handles errors and implements retry logic

## Payment Processing Flow

### 1. Message Consumption Lifecycle

The payment processing follows this detailed lifecycle:

#### Phase 1: Message Reception
```java
@KafkaListener(
    topics = "${app.kafka.topics.payment-request}", 
    containerFactory = "paymentRequestKafkaListenerContainerFactory"
)
@Transactional(transactionManager = "kafkaTransactionManager")
public void consume(@Payload @Valid PaymentDto paymentDto)
```

**When called**: When a message arrives on the `payment-request` topic
**What happens**:
- Message is deserialized to `PaymentDto`
- Bean validation is performed (`@Valid`)
- Transaction context is established
- MDC (Mapped Diagnostic Context) is set for structured logging

#### Phase 2: Balance Verification
```java
boolean balanceCheckResult = balanceCheckClient.checkBalance(
    paymentDto.getCustomerId(),
    paymentDto.getAmount()
);
```

**When called**: After successful message consumption
**What happens**:
- External balance check service is called
- Can throw: `InsufficientBalanceException`, `AccountNotFoundException`, `ServiceUnavailableException`
- MDC context is set with customer and amount information

#### Phase 3: Payment Execution (if balance sufficient)
```java
paymentExecutionClient.executePayment(paymentDto);
```

**When called**: Only if balance check returns `true`
**What happens**:
- External payment execution service is called
- Can throw: `PaymentProcessingException`, `InvalidPaymentMethodException`, `GatewayTimeoutException`
- MDC context includes payment details

#### Phase 4: Response Generation
**Success Path**:
```java
paymentResponseProducer.sendSuccessResponse(paymentDto);
```

**Error Path**:
```java
paymentResponseProducer.sendErrorResponse(paymentDto, "Error message");
```

**When called**: After payment execution (success) or when balance is insufficient (error)
**What happens**:
- Response message is created with appropriate status
- Message is sent to `payment-response` topic synchronously
- Transaction is committed if successful

## Error Handling and Retry Mechanisms

### Error Handler Configuration

The `PaymentRequestErrorHandler` extends Spring Kafka's `DefaultErrorHandler` and is configured with:

```yaml
app:
  kafka:
    error-handler:
      retry:
        initial-interval: 1000      # 1 second initial delay
        multiplier: 2.0             # Exponential backoff multiplier
        max-interval: 30000         # Maximum 30 seconds between retries
        max-elapsed-time: 300000    # Total retry period: 5 minutes
```

### Retry Behavior by Exception Type

#### Retryable Exceptions
These exceptions trigger the exponential backoff retry mechanism:
- `GatewayTimeoutException` - Network timeouts, temporary service issues
- `ServiceUnavailableException` - Temporary service unavailability
- `DeserializationException` - Message parsing issues
- Any other `RuntimeException` not explicitly marked as non-retryable

**Retry Schedule Example**:
- Attempt 1: Immediate
- Attempt 2: After 1 second
- Attempt 3: After 2 seconds  
- Attempt 4: After 4 seconds
- Attempt 5: After 8 seconds
- Attempt 6: After 16 seconds
- Attempt 7: After 30 seconds (capped at max-interval)
- Continue until max-elapsed-time (5 minutes) is reached

#### Non-Retryable Exceptions
These exceptions immediately trigger the recovery process without retries:
- `InsufficientBalanceException` - Business logic error, retry won't help
- `AccountNotFoundException` - Data issue, retry won't help  
- `PaymentProcessingException` - Business processing error
- `InvalidPaymentMethodException` - Invalid input data

### Error Handler Lifecycle Methods

#### 1. handleOne() Method
```java
public boolean handleOne(Exception thrownException,
                        ConsumerRecord<?, ?> record,
                        Consumer<?, ?> consumer,
                        MessageListenerContainer container)
```

**When called**: For each retry attempt of a failed message
**What happens**:
- Logs the retry attempt with structured logging
- Calls parent implementation to perform actual retry logic
- Returns boolean indicating if retry should continue

#### 2. handleRemaining() Method
```java
public void handleRemaining(Exception thrownException,
                           List<ConsumerRecord<?, ?>> records,
                           Consumer<?, ?> consumer,
                           MessageListenerContainer container)
```

**When called**: After all retry attempts are exhausted
**What happens**:
- Logs final failure with all remaining records
- Calls the configured `ConsumerRecordRecoverer` (PaymentRequestRecoverer)

#### 3. PaymentRequestRecoverer.accept() Method
```java
public void accept(ConsumerRecord<?, ?> record, Exception exception)
```

**When called**: As the final step when all retries are exhausted
**What happens**:
- Attempts to extract `PaymentDto` from the failed record
- Sends error response via `PaymentResponseProducer`
- Handles cases where PaymentDto cannot be extracted (e.g., deserialization failures)

## Exception Types and Handling Strategies

### Business Exceptions (Non-Retryable)

#### InsufficientBalanceException
- **Thrown by**: `BalanceCheckClient.checkBalance()`
- **Scenario**: Customer doesn't have enough balance
- **Handling**: Immediate error response, no retries
- **Response**: Error status with "Insufficient balance" message

#### AccountNotFoundException  
- **Thrown by**: `BalanceCheckClient.checkBalance()`
- **Scenario**: Customer account doesn't exist
- **Handling**: Immediate error response, no retries
- **Response**: Error status with "Account not found" message

#### InvalidPaymentMethodException
- **Thrown by**: `PaymentExecutionClient.executePayment()`
- **Scenario**: Invalid payment method provided
- **Handling**: Immediate error response, no retries
- **Response**: Error status with "Invalid payment method" message

#### PaymentProcessingException
- **Thrown by**: `PaymentExecutionClient.executePayment()`
- **Scenario**: General payment processing failure
- **Handling**: Immediate error response, no retries
- **Response**: Error status with processing error details

### Technical Exceptions (Retryable)

#### GatewayTimeoutException
- **Thrown by**: `PaymentExecutionClient.executePayment()`
- **Scenario**: Payment gateway timeout
- **Handling**: Exponential backoff retries for 5 minutes
- **Rationale**: Temporary network/service issue, likely to resolve

#### ServiceUnavailableException
- **Thrown by**: `BalanceCheckClient.checkBalance()`
- **Scenario**: Balance service temporarily unavailable
- **Handling**: Exponential backoff retries for 5 minutes
- **Rationale**: Service may recover during retry period

## Kafka Configuration Details

### Consumer Configuration
```yaml
spring:
  kafka:
    consumer:
      key-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.json.trusted.packages: "ch.ruyalabs.springkafkalab.dto"
        spring.deserializer.key.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer

app:
  kafka:
    consumer:
      payment-request:
        group-id: payment-request-consumer-group
        auto-offset-reset: "earliest"
        enable-auto-commit: false  # Manual acknowledgment for transaction control
```

**Key Features**:
- **ErrorHandlingDeserializer**: Wraps deserialization errors for proper handling
- **Manual Acknowledgment**: Ensures messages are only acknowledged after successful processing
- **Transactional Processing**: Uses `kafkaTransactionManager` for atomicity

### Producer Configuration
```yaml
app:
  kafka:
    producer:
      acks: "all"                    # Wait for all replicas to acknowledge
      retries: 3                     # Producer-level retries
      enable-idempotence: true       # Prevent duplicate messages
      transactional-id: "payment-response-producer"  # Enable transactions
```

### Topic Configuration
```yaml
app:
  kafka:
    topics:
      payment-request: payment-request
      payment-response: payment-response
      partitions: 3
      replication-factor: 3
```

## Structured Logging and Monitoring

### MDC (Mapped Diagnostic Context) Usage

The application uses SLF4J's MDC to provide structured logging context throughout the processing pipeline:

#### Consumer Level MDC
```java
MDC.put("paymentId", paymentDto.getPaymentId());
MDC.put("customerId", paymentDto.getCustomerId());
MDC.put("amount", paymentDto.getAmount().toString());
MDC.put("currency", paymentDto.getCurrency());
MDC.put("operation", "payment_request_processing");
```

#### Client Level MDC
```java
// Balance Check Client
MDC.put("customerId", customerId);
MDC.put("requiredAmount", requiredAmount.toString());
MDC.put("operation", "balance_check");

// Payment Execution Client  
MDC.put("paymentId", paymentDto.getPaymentId());
MDC.put("paymentMethod", paymentDto.getPaymentMethod().toString());
MDC.put("operation", "payment_execution");
```

#### Error Handler MDC
```java
MDC.put("operation", "error_handling");
MDC.put("attemptType", attemptType);
MDC.put("topic", record.topic());
MDC.put("partition", String.valueOf(record.partition()));
MDC.put("offset", String.valueOf(record.offset()));
```

### Structured Logging Events

The application emits structured log events for monitoring:

#### Success Events
- `payment_request_consumed` - Message received from Kafka
- `balance_check_success` - Balance verification successful
- `payment_execution_success` - Payment executed successfully
- `payment_request_processed` - End-to-end processing complete
- `payment_response_sent` - Response sent to Kafka

#### Error Events
- `balance_check_failed` - Balance verification failed
- `payment_execution_failed` - Payment execution failed
- `error_handler_retry` - Retry attempt logged
- `error_handler_final_failure` - All retries exhausted
- `error_response_sent` - Error response sent
- `payment_response_send_failed` - Failed to send response

### Log Format
```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "logger": "ch.ruyalabs.springkafkalab.consumer.PaymentRequestConsumer",
  "message": "Payment request consumed from Kafka topic",
  "event": "payment_request_consumed",
  "paymentId": "PAY-12345",
  "customerId": "CUST-67890",
  "amount": 100.50,
  "currency": "USD",
  "topic": "payment-request"
}
```

## Transaction Management

The application uses Spring's transaction management with Kafka transactions:

### Transaction Boundaries
1. **Start**: When `@KafkaListener` method begins
2. **Scope**: Includes all processing (balance check, payment execution, response sending)
3. **Commit**: When method completes successfully
4. **Rollback**: When any exception is thrown

### Transaction Manager Configuration
```java
@Transactional(transactionManager = "kafkaTransactionManager")
```

This ensures:
- Consumer offset is only committed on successful processing
- Producer messages are only sent on successful transaction commit
- Failed messages can be retried from the same offset

## Testing and Simulation

The application includes simulation flags for testing different error scenarios:

### Balance Check Simulation
```yaml
app:
  simulation:
    balance-check:
      simulate-insufficient-balance: false
      simulate-account-not-found: false  
      simulate-service-unavailable: false
```

### Payment Execution Simulation
```yaml
app:
  simulation:
    payment-execution:
      simulate-payment-processing-exception: false
      simulate-invalid-payment-method-exception: false
      simulate-gateway-timeout-exception: false
```

**Usage**: Set any flag to `true` to simulate that specific error condition for all requests.

## Running the Application

### Prerequisites
- Java 17+
- Docker and Docker Compose (for Kafka cluster)
- Maven 3.6+

### Starting Kafka Cluster
```bash
docker-compose up -d
```

This starts a 3-node Kafka cluster with Zookeeper.

### Running the Application
```bash
mvn spring-boot:run
```

### Configuration Profiles
The application supports different profiles:
- `default` - Standard configuration
- `test` - Test configuration with different settings

## Monitoring and Observability

### Key Metrics to Monitor
1. **Message Processing Rate** - Messages consumed per second
2. **Error Rate** - Percentage of messages that fail processing
3. **Retry Attempts** - Number of retry attempts per message
4. **Processing Latency** - Time from consumption to response
5. **Transaction Success Rate** - Percentage of successful transactions

### Log Analysis Queries
```bash
# Find all retry attempts
grep "error_handler_retry" logs/spring-kafka-lab.log

# Find final failures
grep "error_handler_final_failure" logs/spring-kafka-lab.log

# Find specific payment processing
grep "paymentId\":\"PAY-12345" logs/spring-kafka-lab.log
```

## Best Practices Implemented

1. **Idempotent Processing** - Producer idempotence prevents duplicate messages
2. **Transactional Messaging** - Ensures exactly-once processing semantics
3. **Structured Logging** - Enables effective monitoring and debugging
4. **Graceful Error Handling** - Distinguishes between retryable and non-retryable errors
5. **Resource Cleanup** - MDC context is always cleared in finally blocks
6. **Configuration Externalization** - All settings configurable via application.yml
7. **Comprehensive Exception Handling** - Specific exceptions for different failure scenarios

This architecture ensures robust, observable, and maintainable payment processing with sophisticated error handling and retry mechanisms.
