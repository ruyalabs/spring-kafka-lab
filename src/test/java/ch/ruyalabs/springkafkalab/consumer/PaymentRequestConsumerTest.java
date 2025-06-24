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
        paymentRequestConsumer.consume(paymentDto);

        // Then
        verify(balanceCheckClient).checkBalance(paymentDto.getCustomerId(), paymentDto.getAmount());
        verify(paymentExecutionClient).requestPaymentExecution(paymentDto);
        // No immediate success response is sent - response will be sent by PaymentExecutionStatusConsumer
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer, never()).sendErrorResponse(any(), any());
    }

    @Test
    void testInsufficientBalanceProcessing() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        when(balanceCheckClient.checkBalance(anyString(), any())).thenReturn(false);

        // When
        paymentRequestConsumer.consume(paymentDto);

        // Then
        verify(balanceCheckClient).checkBalance(paymentDto.getCustomerId(), paymentDto.getAmount());
        verify(paymentExecutionClient, never()).requestPaymentExecution(any());
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer).sendErrorResponse(paymentDto, "Insufficient balance for payment");
    }

    @Test
    void testExceptionDuringProcessingWithSuccessfulErrorResponse() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        when(balanceCheckClient.checkBalance(anyString(), any())).thenThrow(new RuntimeException("Balance check failed"));

        // When & Then - Exception should propagate to DefaultErrorHandler
        Exception exception = assertThrows(RuntimeException.class, () -> {
            paymentRequestConsumer.consume(paymentDto);
        });

        assertEquals("Balance check failed", exception.getMessage());
        verify(balanceCheckClient).checkBalance(paymentDto.getCustomerId(), paymentDto.getAmount());
        verify(paymentExecutionClient, never()).requestPaymentExecution(any());
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer, never()).sendErrorResponse(any(), any());
    }

    @Test
    void testExceptionDuringProcessingWithFailedErrorResponse() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        when(balanceCheckClient.checkBalance(anyString(), any())).thenThrow(new RuntimeException("Balance check failed"));

        // When & Then - Original exception should propagate to DefaultErrorHandler
        Exception exception = assertThrows(RuntimeException.class, () -> {
            paymentRequestConsumer.consume(paymentDto);
        });

        assertEquals("Balance check failed", exception.getMessage());
        verify(balanceCheckClient).checkBalance(paymentDto.getCustomerId(), paymentDto.getAmount());
        verify(paymentExecutionClient, never()).requestPaymentExecution(any());
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer, never()).sendErrorResponse(any(), any());
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
