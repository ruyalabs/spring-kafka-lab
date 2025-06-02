package ch.ruyalabs.springkafkalab.producer;

import ch.qos.logback.classic.Level;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.util.LogCaptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentRequestProducerTest {

    private static final String TOPIC = "test-payment-request";

    @Mock
    private KafkaTemplate<String, PaymentDto> kafkaTemplate;

    @Mock
    private SendResult<String, PaymentDto> sendResult;

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
    void sendPaymentRequest_withValidPayment_shouldSendToKafka() {
        // Arrange
        PaymentDto payment = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", "Test payment");
        CompletableFuture<SendResult<String, PaymentDto>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(anyString(), anyString(), any(PaymentDto.class))).thenReturn(future);

        // Act
        CompletableFuture<SendResult<String, PaymentDto>> result = producer.sendPaymentRequest(payment);

        // Assert
        verify(kafkaTemplate).send(eq(TOPIC), eq(payment.getId()), eq(payment));
        assertNotNull(result);
        assertSame(future, result);
    }

    @Test
    void sendPaymentRequest_withNullId_shouldGenerateRandomId() {
        // Arrange
        PaymentDto payment = new PaymentDto(null, new BigDecimal("100.00"), "USD", "Test payment");
        CompletableFuture<SendResult<String, PaymentDto>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(anyString(), anyString(), any(PaymentDto.class))).thenReturn(future);

        // Act
        producer.sendPaymentRequest(payment);

        // Assert
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(TOPIC), keyCaptor.capture(), eq(payment));

        String capturedKey = keyCaptor.getValue();
        assertNotNull(capturedKey);
        assertFalse(capturedKey.isEmpty());
        // Verify it's a valid UUID
        assertDoesNotThrow(() -> UUID.fromString(capturedKey));
    }

    @Test
    void sendPaymentRequest_futureCompletesSuccessfully_invokesInternalSuccessHandler() throws Exception {
        // Arrange
        PaymentDto payment = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", "Test payment");
        CompletableFuture<SendResult<String, PaymentDto>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), any(PaymentDto.class))).thenReturn(future);

        // Mock SendResult with RecordMetadata
        org.apache.kafka.clients.producer.RecordMetadata recordMetadata = mock(org.apache.kafka.clients.producer.RecordMetadata.class);
        when(recordMetadata.topic()).thenReturn(TOPIC);
        when(recordMetadata.partition()).thenReturn(0);
        when(recordMetadata.offset()).thenReturn(123L);
        when(sendResult.getRecordMetadata()).thenReturn(recordMetadata);

        // Act
        CompletableFuture<SendResult<String, PaymentDto>> result = producer.sendPaymentRequest(payment);

        // Complete the future to trigger the callback
        future.complete(sendResult);

        // Assert
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());

        // Verify logging
        assertEquals(2, logCaptor.getMessages().size()); // Initial log + success log
        assertTrue(logCaptor.getMessages(Level.INFO).get(1).contains("successfully delivered"));
        assertTrue(logCaptor.getMessages(Level.INFO).get(1).contains(TOPIC));
        assertTrue(logCaptor.getMessages(Level.INFO).get(1).contains("partition 0"));
        assertTrue(logCaptor.getMessages(Level.INFO).get(1).contains("offset 123"));
    }

    @Test
    void sendPaymentRequest_futureCompletesExceptionally_invokesInternalFailureHandler() {
        // Arrange
        PaymentDto payment = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", "Test payment");
        CompletableFuture<SendResult<String, PaymentDto>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), any(PaymentDto.class))).thenReturn(future);

        Exception testException = new RuntimeException("Test exception");

        // Act
        CompletableFuture<SendResult<String, PaymentDto>> result = producer.sendPaymentRequest(payment);

        // Complete the future exceptionally to trigger the error callback
        future.completeExceptionally(testException);

        // Assert
        assertTrue(result.isCompletedExceptionally());
        assertThrows(RuntimeException.class, () -> result.join());

        // Verify logging
        assertEquals(2, logCaptor.getMessages().size()); // Initial log + error log
        assertTrue(logCaptor.getMessages(Level.ERROR).get(0).contains("Failed to deliver payment request"));
    }
}
