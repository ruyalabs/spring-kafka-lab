package ch.ruyalabs.springkafkalab.producer;

import ch.qos.logback.classic.Level;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.util.LogCaptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentRequestProducerErrorHandlingTest {

    private static final String TOPIC = "test-payment-request";

    @Mock
    private KafkaTemplate<String, PaymentDto> kafkaTemplate;

    private PaymentRequestProducer producer;

    private LogCaptor logCaptor;

    @BeforeEach
    void setUp() {
        producer = new PaymentRequestProducer(kafkaTemplate);
        ReflectionTestUtils.setField(producer, "topic", TOPIC);
        logCaptor = new LogCaptor(PaymentRequestProducer.class.getName());
    }

    @AfterEach
    void tearDown() {
        logCaptor.stop();
    }

    @Test
    void sendPaymentRequest_whenBrokerUnavailable_shouldHandleFailure() {
        // Arrange
        PaymentDto payment = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", "Error test payment");

        CompletableFuture<SendResult<String, PaymentDto>> future = new CompletableFuture<>();
        future.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("Simulated timeout exception"));

        when(kafkaTemplate.send(anyString(), anyString(), any(PaymentDto.class))).thenReturn(future);

        // Act
        CompletableFuture<SendResult<String, PaymentDto>> result = producer.sendPaymentRequest(payment);

        // Assert
        assertTrue(result.isCompletedExceptionally());
        Exception exception = assertThrows(Exception.class, () -> result.join());
        assertTrue(exception.getCause() instanceof org.apache.kafka.common.errors.TimeoutException);

        // Verify logging
        assertEquals(2, logCaptor.getMessages().size()); // Initial log + error log
        assertTrue(logCaptor.getMessages(Level.ERROR).get(0).contains("Failed to deliver payment request"));
    }

    @Test
    void sendPaymentRequest_withNetworkError_shouldHandleFailure() {
        // Arrange
        PaymentDto payment = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", "Network error test");

        CompletableFuture<SendResult<String, PaymentDto>> future = new CompletableFuture<>();
        future.completeExceptionally(new org.springframework.kafka.KafkaException("Simulated network exception"));

        when(kafkaTemplate.send(anyString(), anyString(), any(PaymentDto.class))).thenReturn(future);

        // Act
        CompletableFuture<SendResult<String, PaymentDto>> result = producer.sendPaymentRequest(payment);

        // Assert
        assertTrue(result.isCompletedExceptionally());
        Exception exception = assertThrows(Exception.class, () -> result.join());
        assertTrue(exception.getCause() instanceof org.springframework.kafka.KafkaException);

        // Verify logging
        assertEquals(2, logCaptor.getMessages().size()); // Initial log + error log
        assertTrue(logCaptor.getMessages(Level.ERROR).get(0).contains("Failed to deliver payment request"));
    }
}
