package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.client.BalanceCheckClient;
import ch.ruyalabs.springkafkalab.client.PaymentExecutionClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentRequestConsumerTest {

    @Mock
    private BalanceCheckClient balanceCheckClient;

    @Mock
    private PaymentExecutionClient paymentExecutionClient;

    @Mock
    private PaymentResponseProducer paymentResponseProducer;

    private PaymentRequestConsumer paymentRequestConsumer;

    @BeforeEach
    void setUp() {
        paymentRequestConsumer = new PaymentRequestConsumer(
                balanceCheckClient,
                paymentExecutionClient,
                paymentResponseProducer
        );
    }

    @Test
    void testSuccessfulPaymentProcessing() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        when(balanceCheckClient.checkBalance(anyString(), any())).thenReturn(true);

        // When
        paymentRequestConsumer.consume(paymentDto, null);

        // Then
        verify(balanceCheckClient).checkBalance(paymentDto.getCustomerId(), paymentDto.getAmount());
        verify(paymentExecutionClient).executePayment(paymentDto);
        verify(paymentResponseProducer).sendSuccessResponse(paymentDto);
        verify(paymentResponseProducer, never()).sendErrorResponse(any(), any());
    }

    @Test
    void testInsufficientBalanceProcessing() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        when(balanceCheckClient.checkBalance(anyString(), any())).thenReturn(false);

        // When
        paymentRequestConsumer.consume(paymentDto, null);

        // Then
        verify(balanceCheckClient).checkBalance(paymentDto.getCustomerId(), paymentDto.getAmount());
        verify(paymentExecutionClient, never()).executePayment(any());
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer).sendErrorResponse(paymentDto, "Insufficient balance for payment");
    }

    @Test
    void testExceptionDuringProcessingWithSuccessfulErrorResponse() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        when(balanceCheckClient.checkBalance(anyString(), any())).thenThrow(new RuntimeException("Balance check failed"));

        // When
        paymentRequestConsumer.consume(paymentDto, null);

        // Then
        verify(balanceCheckClient).checkBalance(paymentDto.getCustomerId(), paymentDto.getAmount());
        verify(paymentExecutionClient, never()).executePayment(any());
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer).sendErrorResponse(eq(paymentDto), contains("Balance check failed"));
    }

    @Test
    void testExceptionDuringProcessingWithFailedErrorResponse() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        when(balanceCheckClient.checkBalance(anyString(), any())).thenThrow(new RuntimeException("Balance check failed"));
        doThrow(new RuntimeException("Kafka unavailable")).when(paymentResponseProducer).sendErrorResponse(any(), any());

        // When & Then
        Exception exception = assertThrows(Exception.class, () -> {
            paymentRequestConsumer.consume(paymentDto, null);
        });

        assertEquals("Kafka unavailable", exception.getMessage());
        verify(balanceCheckClient).checkBalance(paymentDto.getCustomerId(), paymentDto.getAmount());
        verify(paymentExecutionClient, never()).executePayment(any());
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer).sendErrorResponse(eq(paymentDto), contains("Balance check failed"));
    }

    @Test
    void testPoisonPillDetectionWithSuccessfulErrorResponse() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        Integer deliveryAttempt = 4; // Above threshold of 3

        // When
        paymentRequestConsumer.consume(paymentDto, deliveryAttempt);

        // Then
        verify(balanceCheckClient, never()).checkBalance(any(), any());
        verify(paymentExecutionClient, never()).executePayment(any());
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer).sendErrorResponse(eq(paymentDto), contains("poison pill"));
    }

    @Test
    void testPoisonPillDetectionWithFailedErrorResponse() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        Integer deliveryAttempt = 5; // Above threshold of 3
        doThrow(new RuntimeException("Kafka unavailable")).when(paymentResponseProducer).sendErrorResponse(any(), any());

        // When - should not throw exception for poison pill, even if error response fails
        assertDoesNotThrow(() -> {
            paymentRequestConsumer.consume(paymentDto, deliveryAttempt);
        });

        // Then
        verify(balanceCheckClient, never()).checkBalance(any(), any());
        verify(paymentExecutionClient, never()).executePayment(any());
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer).sendErrorResponse(eq(paymentDto), contains("poison pill"));
    }

    @Test
    void testNormalProcessingWithLowDeliveryAttempt() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        Integer deliveryAttempt = 2; // Below threshold of 3
        when(balanceCheckClient.checkBalance(anyString(), any())).thenReturn(true);

        // When
        paymentRequestConsumer.consume(paymentDto, deliveryAttempt);

        // Then - should process normally, not as poison pill
        verify(balanceCheckClient).checkBalance(paymentDto.getCustomerId(), paymentDto.getAmount());
        verify(paymentExecutionClient).executePayment(paymentDto);
        verify(paymentResponseProducer).sendSuccessResponse(paymentDto);
    }

    private PaymentDto createTestPaymentDto() {
        PaymentDto paymentDto = new PaymentDto();
        paymentDto.setPaymentId("test-payment-123");
        paymentDto.setCustomerId("customer-456");
        paymentDto.setAmount(100.00);
        paymentDto.setCurrency("USD");
        paymentDto.setPaymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);
        return paymentDto;
    }
}
