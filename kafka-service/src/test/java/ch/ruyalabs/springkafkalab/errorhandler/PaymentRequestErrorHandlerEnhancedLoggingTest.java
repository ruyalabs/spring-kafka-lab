package ch.ruyalabs.springkafkalab.errorhandler;

import ch.qos.logback.classic.Level;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.producer.PaymentResponseProducer;
import ch.ruyalabs.springkafkalab.util.LogCaptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.ResourceAccessException;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;

@ExtendWith(MockitoExtension.class)
class PaymentRequestErrorHandlerEnhancedLoggingTest {

    @Mock
    private PaymentResponseProducer paymentResponseProducer;

    private PaymentRequestErrorHandler errorHandler;
    private LogCaptor logCaptor;

    @BeforeEach
    void setUp() {
        errorHandler = new PaymentRequestErrorHandler(paymentResponseProducer);
        logCaptor = new LogCaptor("ch.ruyalabs.springkafkalab.errorhandler.PaymentRequestErrorHandler");

        // Mock the producer to avoid actual message sending
        doNothing().when(paymentResponseProducer).sendErrorResponse(anyString(), any(), anyString(), anyBoolean());
    }

    @AfterEach
    void tearDown() {
        logCaptor.stop();
    }

    @Test
    void testEnhancedLoggingForRetryableException() {
        // Given
        PaymentDto payment = new PaymentDto();
        payment.setId("test-payment-123");

        ConsumerRecord<String, PaymentDto> record = new ConsumerRecord<>(
            "payment-request", 0, 100L, "test-key", payment);

        ResourceAccessException exception = new ResourceAccessException("Network timeout");

        // When
        errorHandler.handleOne(exception, record, null, null);

        // Then
        assertThat(logCaptor.getMessages(Level.ERROR))
            .anyMatch(log -> log.contains("Payment processing error - Attempt 1/3"))
            .anyMatch(log -> log.contains("paymentId=test-payment-123"))
            .anyMatch(log -> log.contains("retryable=true"))
            .anyMatch(log -> log.contains("exceptionType=ResourceAccessException"));

        assertThat(logCaptor.getMessages(Level.WARN))
            .anyMatch(log -> log.contains("Will retry payment processing"));
    }

    @Test
    void testEnhancedLoggingForNonRetryableException() {
        // Given
        PaymentDto payment = new PaymentDto();
        payment.setId("test-payment-456");

        ConsumerRecord<String, PaymentDto> record = new ConsumerRecord<>(
            "payment-request", 0, 101L, "test-key", payment);

        AccountBalanceClient.AccountNotFoundException exception = 
            new AccountBalanceClient.AccountNotFoundException("Account not found");

        // When
        errorHandler.handleOne(exception, record, null, null);

        // Then
        assertThat(logCaptor.getMessages(Level.ERROR))
            .anyMatch(log -> log.contains("Payment processing error - Attempt 1/3"))
            .anyMatch(log -> log.contains("paymentId=test-payment-456"))
            .anyMatch(log -> log.contains("retryable=false"))
            .anyMatch(log -> log.contains("exceptionType=AccountNotFoundException"))
            .anyMatch(log -> log.contains("Exception is non-retryable"));
    }

    @Test
    void testEnhancedLoggingForExceptionWithCause() {
        // Given
        PaymentDto payment = new PaymentDto();
        payment.setId("test-payment-789");

        ConsumerRecord<String, PaymentDto> record = new ConsumerRecord<>(
            "payment-request", 0, 102L, "test-key", payment);

        IOException rootCause = new IOException("Root cause message");
        ResourceAccessException exception = new ResourceAccessException("Network error", rootCause);

        // When
        errorHandler.handleOne(exception, record, null, null);

        // Then
        assertThat(logCaptor.getMessages(Level.ERROR))
            .anyMatch(log -> log.contains("Exception cause level 1"))
            .anyMatch(log -> log.contains("type=IOException"))
            .anyMatch(log -> log.contains("Root cause message"));
    }
}
