# Spring Kafka Lab - Payment Processing Application

## Overview

This Spring Boot application demonstrates a sophisticated payment processing system using Apache Kafka for asynchronous message handling. The application processes payment requests through multiple stages with robust error handling, transaction management, and resilience patterns.

## Application Flow - Complete Detailed Walkthrough

### Starting Point: External Message to "payment-request" Topic

When an external application writes a message to the **"payment-request"** topic, the following detailed flow is triggered:

### 1. Payment Request Consumption (`PaymentRequestConsumer`)

**Location**: `PaymentRequestConsumer.consume()`
**Topic**: `payment-request`
**Transaction**: Kafka transactional (with `kafkaTransactionManager`)

#### What Happens:
1. **Message Reception**: The `@KafkaListener` receives a `PaymentDto` message from the `payment-request` topic
2. **Validation**: The message is validated using `@Valid` annotation
3. **Logging**: Initial consumption is logged with payment details
4. **Balance Check**: Calls `BalanceCheckClient.checkBalance()` with customer ID and required amount

#### Possible Outcomes:

##### A. Balance Check Successful
- **Action**: Proceeds to payment execution
- **Steps**:
  1. Adds payment to pending payments map: `PaymentExecutionStatusConsumer.addPendingPayment()`
  2. Requests payment execution: `PaymentExecutionClient.requestPaymentExecution()`
  3. Logs successful execution request
- **Next**: Waits for payment execution status

##### B. Balance Check Failed - Insufficient Balance
- **Exception**: `InsufficientBalanceException` or `balanceCheckResult = false`
- **Action**: Immediate error response
- **Steps**:
  1. Logs insufficient balance warning
  2. Calls `PaymentResponseProducer.sendErrorResponse()` with "Insufficient balance" message
  3. Sends error response to `payment-response` topic **within the same transaction**
- **End**: Flow terminates with error response

##### C. Balance Check Failed - Account Not Found
- **Exception**: `AccountNotFoundException`
- **Action**: Immediate error response
- **Steps**:
  1. Logs account not found error
  2. Calls `PaymentResponseProducer.sendErrorResponse()` with account error message
  3. Sends error response to `payment-response` topic **within the same transaction**
- **End**: Flow terminates with error response

##### D. Balance Check Failed - Service Unavailable
- **Exception**: `ServiceUnavailableException`
- **Action**: Immediate error response (no retry policy)
- **Steps**:
  1. Logs service unavailable error as permanent failure
  2. Calls `PaymentResponseProducer.sendErrorResponse()` with service error message
  3. Sends error response to `payment-response` topic **within the same transaction**
- **End**: Flow terminates with error response

##### E. Unexpected Error
- **Exception**: Any other `Exception`
- **Action**: Immediate error response
- **Steps**:
  1. Logs unexpected error with exception details
  2. Calls `PaymentResponseProducer.sendErrorResponse()` with generic error message
  3. Sends error response to `payment-response` topic **within the same transaction**
- **End**: Flow terminates with error response

### 2. Balance Check Simulation (`BalanceCheckClient`)

**Purpose**: Simulates external balance checking service
**Configuration**: Controlled by `application.yml` simulation flags

#### Simulation Behaviors:
- **Normal Operation**: Returns `true` (sufficient balance)
- **Insufficient Balance**: Throws `InsufficientBalanceException` if `simulate-insufficient-balance: true`
- **Account Not Found**: Throws `AccountNotFoundException` if `simulate-account-not-found: true`
- **Service Unavailable**: Throws `ServiceUnavailableException` if `simulate-service-unavailable: true`

### 3. Payment Execution Request (`PaymentExecutionClient`)

**Purpose**: Simulates requesting payment execution from external payment processor
**Behavior**: 
- Logs the payment execution request details
- Simulates successful submission (no actual external call)
- Does not throw exceptions

### 4. Payment Execution Status Processing (`PaymentExecutionStatusConsumer`)

**Location**: `PaymentExecutionStatusConsumer.consume()`
**Topic**: `payment-execution-status`
**Transaction**: Kafka transactional (with `kafkaTransactionManager`)

#### What Happens:
1. **Message Reception**: Receives `PaymentExecutionStatusDto` from `payment-execution-status` topic
2. **Duplicate Check**: Checks if payment was already processed in `completedPayments` map
3. **Pending Payment Lookup**: Retrieves original payment from `pendingPayments` map
4. **Status Evaluation**: Determines success/failure based on `StatusEnum.OK`
5. **Result Creation**: Creates `PaymentExecutionResult` with original payment and status
6. **State Management**: 
   - Adds result to `pendingResponses` map
   - Removes payment from `pendingPayments` map
7. **Event Publishing**: Publishes `PaymentResponseEvent` with payment ID

#### Edge Cases:
- **Duplicate Message**: If payment already in `completedPayments`, acknowledges without reprocessing
- **Orphaned Status**: If no pending payment found, logs warning and returns (handles out-of-order messages)

### 5. Payment Response Event Handling (`PaymentResponseEventListener`)

**Location**: `PaymentResponseEventListener.handlePaymentResponseEvent()`
**Trigger**: `@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)`
**Timing**: Executes **after** the Kafka transaction commits

#### What Happens:
1. **Event Reception**: Receives `PaymentResponseEvent` after transaction commit
2. **Response Lookup**: Gets `PaymentExecutionResult` from `pendingResponses`
3. **State Transition**: Attempts atomic state change from `PENDING` to `SENDING`
4. **Response Sending**: Uses `NonTransactionalPaymentResponseProducer` to send response
5. **Final State**: Transitions to `SENT` and marks payment as completed

#### State Management:
- **PENDING**: Initial state after status processing
- **SENDING**: Currently being sent (prevents duplicates)
- **SENT**: Successfully sent and completed

#### Edge Cases:
- **Already Sent**: Skips if state is already `SENT`
- **Currently Sending**: Skips if state is `SENDING` (prevents race conditions)
- **Send Failure**: Resets state to `PENDING` for retry by scheduled task

### 6. Non-Transactional Response Sending (`NonTransactionalPaymentResponseProducer`)

**Purpose**: Sends payment responses outside of Kafka transactions
**Topic**: `payment-response`
**Template**: Uses `nonTransactionalKafkaTemplate`

#### Why Non-Transactional:
- Prevents deadlocks with the consuming transaction
- Allows response sending after transaction commit
- Enables retry mechanisms without transaction conflicts

#### Behavior:
- **Success Response**: Creates response with `StatusEnum.SUCCESS`
- **Error Response**: Creates response with `StatusEnum.ERROR` and error message
- **Exception Handling**: Catches and logs exceptions (doesn't throw)

### 7. Resilience and Recovery Mechanisms

#### A. Startup Recovery (`processPendingResponsesOnStartup`)
**Trigger**: `@EventListener(ApplicationReadyEvent.class)`
**Purpose**: Handles responses that were pending when application crashed

**Process**:
1. Checks for pending responses in `pendingResponses` map
2. Logs recovery warning if any found
3. Attempts to send each pending response
4. Handles failures gracefully

#### B. Scheduled Retry (`retryPendingResponses`)
**Schedule**: Every 30 seconds (`@Scheduled(fixedDelay = 30000)`)
**Purpose**: Retries failed response sending

**Logic**:
- **Pending Responses**: Retries responses older than 60 seconds in `PENDING` state
- **Stuck Responses**: Resets responses stuck in `SENDING` state for more than 5 minutes
- **Alerting**: Logs alerts for responses stuck longer than 10 minutes
- **Monitoring**: Provides retry summary statistics

#### C. State Consistency
- **Atomic Operations**: Uses `compareAndSetState()` for thread-safe state transitions
- **Duplicate Prevention**: State checks prevent duplicate response sending
- **Recovery Tracking**: Maintains separate maps for different stages

## Kafka Topics

### 1. payment-request
- **Purpose**: Receives payment requests from external applications
- **Consumer**: `PaymentRequestConsumer`
- **Message Type**: `PaymentDto`
- **Partitions**: 3
- **Replication Factor**: 3

### 2. payment-response
- **Purpose**: Sends payment responses to external applications
- **Producers**: 
  - `PaymentResponseProducer` (transactional, for immediate errors)
  - `NonTransactionalPaymentResponseProducer` (non-transactional, for async responses)
- **Message Type**: `PaymentResponseDto`
- **Partitions**: 3
- **Replication Factor**: 3

### 3. payment-execution-status
- **Purpose**: Receives payment execution status from external payment processor
- **Consumer**: `PaymentExecutionStatusConsumer`
- **Message Type**: `PaymentExecutionStatusDto`
- **Partitions**: 3
- **Replication Factor**: 3

## Transaction Management

### Transactional Flow
1. **Payment Request Processing**: Fully transactional with Kafka transaction manager
2. **Immediate Error Responses**: Sent within the same transaction as consumption
3. **Status Processing**: Transactional consumption with event publishing
4. **Async Response Sending**: Non-transactional to prevent deadlocks

### Transaction Boundaries
- **Request → Error Response**: Single transaction
- **Status Processing → Event Publishing**: Single transaction  
- **Event Handling → Response Sending**: Separate, non-transactional

## Edge Cases and Error Scenarios

### 1. Application Crash Scenarios

#### A. Crash After Consuming Request, Before Balance Check
- **Impact**: Message reprocessed on restart
- **Handling**: Idempotent processing, duplicate detection

#### B. Crash After Balance Check, Before Execution Request
- **Impact**: Payment not added to pending, status message becomes orphaned
- **Handling**: Orphaned status detection and logging

#### C. Crash After Status Processing, Before Response Sending
- **Impact**: Response pending in memory
- **Handling**: Startup recovery mechanism processes pending responses

#### D. Crash During Response Sending
- **Impact**: Response may be partially sent
- **Handling**: State management prevents duplicates, retry mechanism ensures delivery

### 2. Message Ordering Issues

#### A. Status Arrives Before Request Processing
- **Scenario**: Network delays cause status to arrive before request is fully processed
- **Handling**: Orphaned status detection, graceful logging

#### B. Duplicate Status Messages
- **Scenario**: External system sends duplicate status messages
- **Handling**: Completed payments tracking prevents reprocessing

### 3. External Service Failures

#### A. Balance Check Service Down
- **Scenario**: `ServiceUnavailableException` thrown
- **Handling**: Immediate error response, no retry (permanent failure policy)

#### B. Payment Execution Service Issues
- **Scenario**: External payment processor fails
- **Handling**: Error status received, error response sent to client

### 4. Kafka Infrastructure Issues

#### A. Kafka Broker Down
- **Scenario**: Kafka cluster partially unavailable
- **Handling**: Spring Kafka retry mechanisms, transaction rollback

#### B. Topic Partition Issues
- **Scenario**: Partition leadership changes
- **Handling**: Automatic reconnection, message redelivery

### 5. Memory and Resource Issues

#### A. High Memory Usage from Pending Maps
- **Scenario**: Large number of pending payments/responses
- **Handling**: Cleanup after completion, monitoring alerts for stuck payments

#### B. Thread Pool Exhaustion
- **Scenario**: High message volume overwhelms consumers
- **Handling**: Kafka consumer configuration limits, backpressure

## Configuration

### Kafka Consumer Settings
- **Group ID**: `payment-request-consumer-group`
- **Auto Offset Reset**: `earliest`
- **Auto Commit**: Disabled (manual transaction management)

### Kafka Producer Settings
- **Acknowledgments**: `all` (wait for all replicas)
- **Retries**: 3
- **Idempotence**: Enabled
- **Transaction Timeout**: 120 seconds

### Simulation Settings
```yaml
app:
  simulation:
    balance-check:
      simulate-insufficient-balance: false
      simulate-account-not-found: false  
      simulate-service-unavailable: false
```

## Monitoring and Observability

### Key Log Messages
- **Request Processing**: Payment request consumption and processing steps
- **Balance Check**: Success/failure with customer details
- **Execution Status**: Payment execution results
- **Response Sending**: Success/failure of response delivery
- **Recovery Operations**: Startup recovery and retry attempts
- **Error Conditions**: All exception scenarios with context

### Metrics to Monitor
- Pending payments count
- Completed payments count
- Response retry attempts
- Stuck response alerts
- Transaction rollback rates
- Consumer lag per topic

## Data Flow Summary

```
External App → payment-request → PaymentRequestConsumer
                                        ↓
                                 BalanceCheckClient
                                        ↓
                              PaymentExecutionClient
                                        ↓
External System → payment-execution-status → PaymentExecutionStatusConsumer
                                                      ↓
                                            PaymentResponseEvent
                                                      ↓
                                        PaymentResponseEventListener
                                                      ↓
                                 NonTransactionalPaymentResponseProducer
                                                      ↓
                                            payment-response → External App
```

This application demonstrates advanced Kafka patterns including transactional messaging, event-driven architecture, resilience patterns, and comprehensive error handling for production-ready payment processing systems.