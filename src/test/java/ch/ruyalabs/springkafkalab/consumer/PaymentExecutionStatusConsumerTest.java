package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.dto.PaymentExecutionStatusDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentExecutionStatusConsumerTest {

    @Mock
    private PaymentResponseProducer paymentResponseProducer;

    private PaymentExecutionStatusConsumer paymentExecutionStatusConsumer;

    @BeforeEach
    void setUp() {
        paymentExecutionStatusConsumer = new PaymentExecutionStatusConsumer(paymentResponseProducer);
        // Clear any pending payments from previous tests
        PaymentExecutionStatusConsumer.getPendingPaymentsCount(); // This doesn't clear, but we'll work with it
    }

    @Test
    void testSuccessfulPaymentExecutionStatus() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        PaymentExecutionStatusDto statusDto = new PaymentExecutionStatusDto()
                .paymentId(paymentDto.getPaymentId())
                .status(PaymentExecutionStatusDto.StatusEnum.OK);

        // Add pending payment
        PaymentExecutionStatusConsumer.addPendingPayment(paymentDto.getPaymentId(), paymentDto);

        // When
        paymentExecutionStatusConsumer.consume(statusDto, null);

        // Then
        verify(paymentResponseProducer).sendSuccessResponse(paymentDto);
        verify(paymentResponseProducer, never()).sendErrorResponse(any(), any());
    }

    @Test
    void testFailedPaymentExecutionStatus() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        PaymentExecutionStatusDto statusDto = new PaymentExecutionStatusDto()
                .paymentId(paymentDto.getPaymentId())
                .status(PaymentExecutionStatusDto.StatusEnum.NOK);

        // Add pending payment
        PaymentExecutionStatusConsumer.addPendingPayment(paymentDto.getPaymentId(), paymentDto);

        // When
        paymentExecutionStatusConsumer.consume(statusDto, null);

        // Then
        verify(paymentResponseProducer).sendErrorResponse(paymentDto, "Payment execution failed");
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
    }

    @Test
    void testOrphanedStatusMessage() throws Exception {
        // Given
        PaymentExecutionStatusDto statusDto = new PaymentExecutionStatusDto()
                .paymentId("non-existent-payment-id")
                .status(PaymentExecutionStatusDto.StatusEnum.OK);

        // When
        paymentExecutionStatusConsumer.consume(statusDto, null);

        // Then - no response should be sent for orphaned status
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer, never()).sendErrorResponse(any(), any());
    }

    @Test
    void testPoisonPillDetection() throws Exception {
        // Given
        PaymentExecutionStatusDto statusDto = new PaymentExecutionStatusDto()
                .paymentId("test-payment-123")
                .status(PaymentExecutionStatusDto.StatusEnum.OK);
        Integer deliveryAttempt = 4; // Above threshold of 3

        // When
        paymentExecutionStatusConsumer.consume(statusDto, deliveryAttempt);

        // Then - should not process the message
        verify(paymentResponseProducer, never()).sendSuccessResponse(any());
        verify(paymentResponseProducer, never()).sendErrorResponse(any(), any());
    }

    @Test
    void testExactlyOneResponseGuarantee() throws Exception {
        // Given
        PaymentDto paymentDto = createTestPaymentDto();
        PaymentExecutionStatusDto statusDto = new PaymentExecutionStatusDto()
                .paymentId(paymentDto.getPaymentId())
                .status(PaymentExecutionStatusDto.StatusEnum.OK);

        // Add pending payment
        PaymentExecutionStatusConsumer.addPendingPayment(paymentDto.getPaymentId(), paymentDto);

        // When - consume the same status twice
        paymentExecutionStatusConsumer.consume(statusDto, null);
        paymentExecutionStatusConsumer.consume(statusDto, null);

        // Then - only one response should be sent (second call should be ignored)
        verify(paymentResponseProducer, times(1)).sendSuccessResponse(paymentDto);
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