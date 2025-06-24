package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.dto.PaymentExecutionStatusDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentExecutionStatusConsumerTest {

    @Mock
    private PaymentResponseProducer paymentResponseProducer;

    @Mock
    private NonTransactionalPaymentResponseProducer nonTransactionalPaymentResponseProducer;

    private PaymentExecutionStatusConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new PaymentExecutionStatusConsumer(paymentResponseProducer, nonTransactionalPaymentResponseProducer);

        // Clear static maps before each test
        PaymentExecutionStatusConsumer.clearAllPayments();

        // Clear pendingResponses map using reflection
        ConcurrentHashMap<String, Object> pendingResponses = 
            (ConcurrentHashMap<String, Object>) ReflectionTestUtils.getField(consumer, "pendingResponses");
        if (pendingResponses != null) {
            pendingResponses.clear();
        }
    }

    @Test
    void testAtomicStateTransitions() throws Exception {
        // Arrange
        String paymentId = "test-payment-123";
        PaymentDto paymentDto = createTestPaymentDto(paymentId);
        PaymentExecutionStatusDto statusDto = createTestStatusDto(paymentId, PaymentExecutionStatusDto.StatusEnum.OK);

        // Add pending payment
        PaymentExecutionStatusConsumer.addPendingPayment(paymentId, paymentDto);

        // Mock the async response producer to simulate a delay
        doAnswer(invocation -> {
            Thread.sleep(50); // Small delay to allow test to check intermediate state
            return null;
        }).when(nonTransactionalPaymentResponseProducer).sendSuccessResponse(any(PaymentDto.class));

        // Act
        consumer.consume(statusDto);

        // Assert immediate state after consumption
        assertEquals(0, PaymentExecutionStatusConsumer.getPendingPaymentsCount(), 
                "Payment should be removed from pending after consumption");

        // Give a moment for the async method to start but not complete
        Thread.sleep(10);

        // Verify that pendingResponses contains the result (before async completion)
        ConcurrentHashMap<String, Object> pendingResponses = 
            (ConcurrentHashMap<String, Object>) ReflectionTestUtils.getField(consumer, "pendingResponses");

        // The payment might be in SENDING state or already completed depending on timing
        // Let's check if it was processed (either still pending or completed)
        boolean wasProcessed = pendingResponses.containsKey(paymentId) || 
                              PaymentExecutionStatusConsumer.getCompletedPaymentsCount() > 0;
        assertTrue(wasProcessed, "Payment should have been processed (either pending or completed)");

        // Wait for async processing to complete
        Thread.sleep(100);

        // Verify final state - should be completed and removed from pending
        assertEquals(1, PaymentExecutionStatusConsumer.getCompletedPaymentsCount(),
                "Payment should be marked as completed after async processing");
        assertFalse(pendingResponses.containsKey(paymentId),
                "Payment should be removed from pending responses after completion");
    }

    @Test
    void testSuccessfulResponseSending() throws Exception {
        // Arrange
        String paymentId = "test-payment-success";
        PaymentDto paymentDto = createTestPaymentDto(paymentId);

        // Create a PaymentExecutionResult and add it to pendingResponses
        Object result = createPaymentExecutionResult(paymentDto, true, null);
        ConcurrentHashMap<String, Object> pendingResponses = 
            (ConcurrentHashMap<String, Object>) ReflectionTestUtils.getField(consumer, "pendingResponses");
        pendingResponses.put(paymentId, result);

        // Act
        consumer.sendPendingResponseAsync(paymentId);

        // Wait a bit for async processing
        Thread.sleep(100);

        // Assert
        verify(nonTransactionalPaymentResponseProducer, times(1))
                .sendSuccessResponse(any(PaymentDto.class));

        assertEquals(1, PaymentExecutionStatusConsumer.getCompletedPaymentsCount(),
                "Payment should be marked as completed");

        assertFalse(pendingResponses.containsKey(paymentId),
                "Payment should be removed from pending responses");
    }

    @Test
    void testFailedResponseSending() throws Exception {
        // Arrange
        String paymentId = "test-payment-failure";
        PaymentDto paymentDto = createTestPaymentDto(paymentId);

        // Create a PaymentExecutionResult and add it to pendingResponses
        Object result = createPaymentExecutionResult(paymentDto, false, "Test error");
        ConcurrentHashMap<String, Object> pendingResponses = 
            (ConcurrentHashMap<String, Object>) ReflectionTestUtils.getField(consumer, "pendingResponses");
        pendingResponses.put(paymentId, result);

        // Mock failure
        doThrow(new RuntimeException("Kafka send failed"))
                .when(nonTransactionalPaymentResponseProducer)
                .sendErrorResponse(any(PaymentDto.class), anyString());

        // Act
        consumer.sendPendingResponseAsync(paymentId);

        // Wait a bit for async processing
        Thread.sleep(100);

        // Assert
        verify(nonTransactionalPaymentResponseProducer, times(1))
                .sendErrorResponse(any(PaymentDto.class), anyString());

        assertEquals(0, PaymentExecutionStatusConsumer.getCompletedPaymentsCount(),
                "Payment should not be marked as completed on failure");

        assertTrue(pendingResponses.containsKey(paymentId),
                "Payment should remain in pending responses for retry");

        // Verify state is reset to PENDING
        Object state = ReflectionTestUtils.invokeMethod(result, "getState");
        assertEquals(PaymentExecutionStatusConsumer.ResponseState.PENDING, state,
                "State should be reset to PENDING after failure");
    }

    @Test
    void testDuplicateProcessingPrevention() throws Exception {
        // Arrange
        String paymentId = "test-payment-duplicate";
        PaymentDto paymentDto = createTestPaymentDto(paymentId);
        PaymentExecutionStatusDto statusDto = createTestStatusDto(paymentId, PaymentExecutionStatusDto.StatusEnum.OK);

        // Add pending payment and mark as completed
        PaymentExecutionStatusConsumer.addPendingPayment(paymentId, paymentDto);
        ConcurrentHashMap<String, Boolean> completedPayments = 
            (ConcurrentHashMap<String, Boolean>) ReflectionTestUtils.getField(consumer, "completedPayments");
        completedPayments.put(paymentId, true);

        // Act
        consumer.consume(statusDto);

        // Assert
        assertEquals(1, PaymentExecutionStatusConsumer.getPendingPaymentsCount(),
                "Payment should remain in pending as it was already completed");

        ConcurrentHashMap<String, Object> pendingResponses = 
            (ConcurrentHashMap<String, Object>) ReflectionTestUtils.getField(consumer, "pendingResponses");
        assertFalse(pendingResponses.containsKey(paymentId),
                "No new pending response should be created for duplicate");
    }

    @Test
    void testRetryMechanism() {
        // Arrange
        String paymentId = "test-payment-retry";
        PaymentDto paymentDto = createTestPaymentDto(paymentId);

        // Create a PaymentExecutionResult with old timestamp
        Object result = createPaymentExecutionResult(paymentDto, true, null);
        // Set timestamp to 2 minutes ago
        ReflectionTestUtils.setField(result, "createdTimestamp", System.currentTimeMillis() - 120000);

        ConcurrentHashMap<String, Object> pendingResponses = 
            (ConcurrentHashMap<String, Object>) ReflectionTestUtils.getField(consumer, "pendingResponses");
        pendingResponses.put(paymentId, result);

        // Act
        consumer.retryPendingResponses();

        // Assert
        verify(nonTransactionalPaymentResponseProducer, times(1))
                .sendSuccessResponse(any(PaymentDto.class));
    }

    private PaymentDto createTestPaymentDto(String paymentId) {
        PaymentDto paymentDto = new PaymentDto();
        paymentDto.setPaymentId(paymentId);
        paymentDto.setCustomerId("customer-123");
        paymentDto.setAmount(100.00);
        paymentDto.setCurrency("USD");
        paymentDto.setPaymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);
        return paymentDto;
    }

    private PaymentExecutionStatusDto createTestStatusDto(String paymentId, PaymentExecutionStatusDto.StatusEnum status) {
        PaymentExecutionStatusDto statusDto = new PaymentExecutionStatusDto();
        statusDto.setPaymentId(paymentId);
        statusDto.setStatus(status);
        return statusDto;
    }

    private Object createPaymentExecutionResult(PaymentDto paymentDto, boolean isSuccess, String errorMessage) {
        try {
            // Use reflection to create PaymentExecutionResult instance
            Class<?> resultClass = Class.forName("ch.ruyalabs.springkafkalab.consumer.PaymentExecutionStatusConsumer$PaymentExecutionResult");
            return resultClass.getDeclaredConstructor(PaymentDto.class, boolean.class, String.class)
                    .newInstance(paymentDto, isSuccess, errorMessage);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create PaymentExecutionResult", e);
        }
    }
}
