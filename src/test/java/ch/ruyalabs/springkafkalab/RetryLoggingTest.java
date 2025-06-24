package ch.ruyalabs.springkafkalab;

import ch.ruyalabs.springkafkalab.config.PaymentRequestRetryListener;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.exception.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.exception.ServiceUnavailableException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class RetryLoggingTest {

    private PaymentRequestRetryListener retryListener;

    @BeforeEach
    void setUp() {
        retryListener = new PaymentRequestRetryListener();
    }

    @Test
    void testRetryLoggingForRetryableException() {
        System.out.println("[DEBUG_LOG] Testing retry logging for retryable exception");

        // Create a test PaymentDto
        PaymentDto paymentDto = new PaymentDto()
                .paymentId("retry_test_payment_123")
                .customerId("retry_test_customer_456")
                .amount(100.00)
                .currency("USD")
                .paymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);

        // Create a mock ConsumerRecord
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
                "payment-request-test", 0, 0L, "test-key", paymentDto);

        // Create a retryable exception
        ServiceUnavailableException exception = new ServiceUnavailableException("Service temporarily unavailable");

        // Test the failedDelivery method
        assertDoesNotThrow(() -> {
            retryListener.failedDelivery(record, exception, 1);
            retryListener.failedDelivery(record, exception, 2);
            retryListener.failedDelivery(record, exception, 3);
        });

        System.out.println("[DEBUG_LOG] Retryable exception logging test completed successfully");
        System.out.println("[DEBUG_LOG] Check logs above for retry messages with:");
        System.out.println("[DEBUG_LOG] - 'Retrying message for the 1st time'");
        System.out.println("[DEBUG_LOG] - 'Retrying message for the 2nd time'");
        System.out.println("[DEBUG_LOG] - 'Retrying message for the 3rd time'");
        System.out.println("[DEBUG_LOG] - ExceptionType: ServiceUnavailableException (RETRYABLE)");
    }

    @Test
    void testRetryLoggingForNonRetryableException() {
        System.out.println("[DEBUG_LOG] Testing retry logging for non-retryable exception");

        // Create a test PaymentDto
        PaymentDto paymentDto = new PaymentDto()
                .paymentId("non_retry_test_payment_789")
                .customerId("non_retry_test_customer_999")
                .amount(100.00)
                .currency("USD")
                .paymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);

        // Create a mock ConsumerRecord
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
                "payment-request-test", 0, 0L, "test-key", paymentDto);

        // Create a non-retryable exception
        AccountNotFoundException exception = new AccountNotFoundException("Account not found");

        // Test the failedDelivery method
        assertDoesNotThrow(() -> {
            retryListener.failedDelivery(record, exception, 1);
        });

        System.out.println("[DEBUG_LOG] Non-retryable exception logging test completed successfully");
        System.out.println("[DEBUG_LOG] Check logs above for non-retryable message with:");
        System.out.println("[DEBUG_LOG] - 'A non-retryable exception was caught'");
        System.out.println("[DEBUG_LOG] - ExceptionType: AccountNotFoundException (NON_RETRYABLE)");
    }

    @Test
    void testRecoveredLogging() {
        System.out.println("[DEBUG_LOG] Testing recovery logging");

        // Create a test PaymentDto
        PaymentDto paymentDto = new PaymentDto()
                .paymentId("recovery_test_payment_456")
                .customerId("recovery_test_customer_789")
                .amount(50.00)
                .currency("EUR")
                .paymentMethod(PaymentDto.PaymentMethodEnum.BANK_TRANSFER);

        // Create a mock ConsumerRecord
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
                "payment-request-test", 0, 0L, "test-key", paymentDto);

        // Create an exception
        ServiceUnavailableException exception = new ServiceUnavailableException("Service recovered");

        // Test the recovered method
        assertDoesNotThrow(() -> {
            retryListener.recovered(record, exception);
        });

        System.out.println("[DEBUG_LOG] Recovery logging test completed successfully");
        System.out.println("[DEBUG_LOG] Check logs above for recovery message");
    }
}
