# Spring Kafka Lab - Payment Processing System

## Overview

This is a Spring Boot application that demonstrates an asynchronous payment processing system using Apache Kafka. The application implements a distributed payment flow where payment execution requests are handled by external systems, and responses are processed asynchronously through Kafka topics.

## Architecture

The application follows an event-driven architecture with the following key components:

### Core Components

1. **PaymentRequestConsumer** - Consumes payment requests and validates balance
2. **PaymentExecutionClient** - Submits payment execution requests to external systems
3. **PaymentExecutionStatusConsumer** - Processes execution status from external systems
4. **PaymentResponseProducer** - Sends final payment responses
5. **BalanceCheckClient** - Validates customer balance before processing

### Kafka Topics

The application uses three main Kafka topics:

- **payment-request** - Receives initial payment requests
- **payment-execution-status** - Receives execution status from external payment processors
- **payment-response** - Sends final payment processing results

## Payment Processing Flow

The payment processing follows this precise flow:

### 1. Payment Request Initiation
- A payment request is published to the `payment-request` topic
- The request contains: paymentId, customerId, amount, currency, paymentMethod

### 2. Payment Request Processing
- **PaymentRequestConsumer** consumes the payment request
- Performs balance check using **BalanceCheckClient**
- If balance is insufficient:
  - Sends error response immediately via **PaymentResponseProducer**
  - Flow ends here
- If balance is sufficient:
  - Calls **PaymentExecutionClient.requestPaymentExecution()**
  - Stores the payment as "pending" in **PaymentExecutionStatusConsumer**
  - Waits for external system response

### 3. External Payment Execution
- **PaymentExecutionClient** only submits a REQUEST for payment execution
- An external payment processing system handles the actual execution
- The external system publishes the execution result to `payment-execution-status` topic

### 4. Execution Status Processing
- **PaymentExecutionStatusConsumer** consumes from `payment-execution-status` topic
- Matches the status with the pending payment using paymentId
- Based on execution status:
  - **OK**: Sends success response via **PaymentResponseProducer**
  - **NOK**: Sends error response via **PaymentResponseProducer**
- Removes the payment from pending list

### 5. Final Response
- **PaymentResponseProducer** publishes the final result to `payment-response` topic
- Response contains: paymentId, customerId, amount, status (success/error), errorInfo (if applicable)

## Key Guarantees

### Exactly-Once Response
- Each payment request results in exactly one response
- Duplicate status messages are ignored (orphaned messages)
- Pending payments are removed after processing to prevent duplicate responses

### Error Handling
- Poison pill detection (messages retried more than 3 times)
- Balance validation before execution requests
- Graceful handling of orphaned status messages
- Transaction management with Kafka transactions

## Configuration

### Kafka Configuration
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:29092,localhost:29093,localhost:29094
    consumer:
      key-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer

app:
  kafka:
    topics:
      payment-request: payment-request
      payment-response: payment-response
      payment-execution-status: payment-execution-status
      partitions: 3
      replication-factor: 3
```

### Simulation Settings
The application includes simulation flags for testing various scenarios:
- Balance check failures
- Account not found errors
- Service unavailability
- Payment processing exceptions
- Gateway timeouts

## Data Models

### PaymentDto
- paymentId: Unique payment identifier
- customerId: Customer identifier
- amount: Payment amount
- currency: ISO currency code
- paymentMethod: Payment method (CREDIT_CARD, DEBIT_CARD, etc.)

### PaymentExecutionStatusDto
- paymentId: Payment identifier
- status: Execution status (OK/NOK)

### PaymentResponseDto
- paymentId: Payment identifier
- customerId: Customer identifier
- amount: Payment amount
- currency: Currency code
- paymentMethod: Payment method
- status: Final status (success/error)
- errorInfo: Error details (if applicable)

## Running the Application

### Prerequisites
- Java 17+
- Apache Kafka cluster running on ports 29092, 29093, 29094
- Maven 3.6+

### Startup
1. Start Kafka cluster using the provided docker-compose.yml
2. Run the Spring Boot application:
   ```bash
   mvn spring-boot:run
   ```

### Docker Compose
The application includes a docker-compose.yml for running Kafka locally.

## Testing

The application includes comprehensive unit tests:

### PaymentRequestConsumerTest
- Tests successful payment processing flow
- Tests insufficient balance scenarios
- Tests poison pill detection
- Tests exception handling

### PaymentExecutionStatusConsumerTest
- Tests successful execution status processing
- Tests failed execution status processing
- Tests orphaned status message handling
- Tests exactly-once response guarantee

### Running Tests
```bash
mvn test
```

## Monitoring and Logging

The application provides detailed logging for:
- Payment request processing
- Balance check operations
- Payment execution requests
- Status message processing
- Response generation
- Error conditions

All log messages include relevant identifiers (paymentId, customerId) for traceability.

## Flow Diagram

```
[Payment Request] 
       ↓
[PaymentRequestConsumer]
       ↓
[Balance Check] → [Insufficient] → [Error Response]
       ↓ [Sufficient]
[PaymentExecutionClient.requestPaymentExecution()]
       ↓
[Store as Pending Payment]
       ↓
[External Payment System] → [payment-execution-status topic]
       ↓
[PaymentExecutionStatusConsumer]
       ↓
[Match with Pending Payment]
       ↓
[Send Success/Error Response] → [payment-response topic]
```

This architecture ensures reliable, asynchronous payment processing with proper error handling and exactly-once delivery guarantees.