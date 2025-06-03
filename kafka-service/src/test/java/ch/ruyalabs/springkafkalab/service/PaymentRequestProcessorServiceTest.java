package ch.ruyalabs.springkafkalab.service;

import ch.ruyalabs.springkafkalab.client.AccountBalanceClient;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.AccountBalance;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.InsufficientFundsException;
import ch.ruyalabs.springkafkalab.client.BookingClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.producer.PaymentRequestProducer;
import ch.ruyalabs.springkafkalab.util.LogCaptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentRequestProcessorServiceTest {

    @Mock
    private PaymentRequestProducer paymentRequestProducer;

    @Mock
    private AccountBalanceClient accountBalanceClient;

    @Mock
    private BookingClient bookingClient;

    @Mock
    private SendResult<String, PaymentDto> sendResult;

    private PaymentRequestProcessorService service;

    private LogCaptor logCaptor;

    @BeforeEach
    void setUp() {
        service = new PaymentRequestProcessorService(paymentRequestProducer, accountBalanceClient, bookingClient);
        logCaptor = new LogCaptor(PaymentRequestProcessorService.class.getName());
    }

    @AfterEach
    void tearDown() {
        logCaptor.stop();
    }

    @Test
    void processPayment_withValidPayment_shouldProcessSuccessfully() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", "Test payment");
        AccountBalance accountBalance = new AccountBalance("account-1", new BigDecimal("500.00"), "USD");

        when(accountBalanceClient.getBalance(anyString())).thenReturn(accountBalance);
        when(paymentRequestProducer.sendPaymentRequest(any(PaymentDto.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // Act
        PaymentDto result = service.processPayment(paymentDto);

        // Assert
        assertEquals(paymentDto, result);
        verify(accountBalanceClient).getBalance(anyString());
        verify(paymentRequestProducer).sendPaymentRequest(paymentDto);
    }

    @Test
    void processPayment_withNullId_shouldGenerateId() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto(null, new BigDecimal("100.00"), "USD", "Test payment");
        AccountBalance accountBalance = new AccountBalance("account-1", new BigDecimal("500.00"), "USD");

        when(accountBalanceClient.getBalance(anyString())).thenReturn(accountBalance);
        when(paymentRequestProducer.sendPaymentRequest(any(PaymentDto.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // Act
        PaymentDto result = service.processPayment(paymentDto);

        // Assert
        assertNotNull(result.getId());
        assertFalse(result.getId().isEmpty());
        // Verify it's a valid UUID
        assertDoesNotThrow(() -> UUID.fromString(result.getId()));
        verify(accountBalanceClient).getBalance(anyString());
        verify(paymentRequestProducer).sendPaymentRequest(paymentDto);
    }

    @Test
    void processPayment_withNullDescription_shouldSetDefaultDescription() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", null);
        AccountBalance accountBalance = new AccountBalance("account-1", new BigDecimal("500.00"), "USD");

        when(accountBalanceClient.getBalance(anyString())).thenReturn(accountBalance);
        when(paymentRequestProducer.sendPaymentRequest(any(PaymentDto.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // Act
        PaymentDto result = service.processPayment(paymentDto);

        // Assert
        assertNotNull(result.getDescription());
        assertEquals("Payment " + paymentDto.getId(), result.getDescription());
        verify(accountBalanceClient).getBalance(anyString());
        verify(paymentRequestProducer).sendPaymentRequest(paymentDto);
    }

    @Test
    void processPayment_withNegativeAmount_shouldThrowException() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("-100.00"), "USD", "Test payment");

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
                () -> service.processPayment(paymentDto));
        assertEquals("Payment amount must be positive", exception.getMessage());

        // Verify no interactions with dependencies
        verifyNoInteractions(accountBalanceClient);
        verifyNoInteractions(paymentRequestProducer);
    }

    @Test
    void processPayment_withNullAmount_shouldThrowException() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", null, "USD", "Test payment");

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
                () -> service.processPayment(paymentDto));
        assertEquals("Payment amount must be positive", exception.getMessage());

        // Verify no interactions with dependencies
        verifyNoInteractions(accountBalanceClient);
        verifyNoInteractions(paymentRequestProducer);
    }

    @Test
    void processPayment_withNullCurrency_shouldThrowException() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("100.00"), null, "Test payment");

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
                () -> service.processPayment(paymentDto));
        assertEquals("Payment currency is required", exception.getMessage());

        // Verify no interactions with dependencies
        verifyNoInteractions(accountBalanceClient);
        verifyNoInteractions(paymentRequestProducer);
    }

    @Test
    void processPayment_withEmptyCurrency_shouldThrowException() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("100.00"), "", "Test payment");

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, 
                () -> service.processPayment(paymentDto));
        assertEquals("Payment currency is required", exception.getMessage());

        // Verify no interactions with dependencies
        verifyNoInteractions(accountBalanceClient);
        verifyNoInteractions(paymentRequestProducer);
    }

    @Test
    void processPayment_withInsufficientFunds_shouldThrowException() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("1000.00"), "USD", "Test payment");
        AccountBalance accountBalance = new AccountBalance("account-1", new BigDecimal("500.00"), "USD");

        when(accountBalanceClient.getBalance(anyString())).thenReturn(accountBalance);

        // Act & Assert
        InsufficientFundsException exception = assertThrows(InsufficientFundsException.class, 
                () -> service.processPayment(paymentDto));
        assertTrue(exception.getMessage().contains("Insufficient funds for payment"));

        // Verify interactions
        verify(accountBalanceClient).getBalance(anyString());
        verifyNoInteractions(paymentRequestProducer);
    }

    @Test
    void processPayment_withCurrencyMismatch_shouldStillProcessPayment() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("100.00"), "EUR", "Test payment");
        AccountBalance accountBalance = new AccountBalance("account-1", new BigDecimal("500.00"), "USD");

        when(accountBalanceClient.getBalance(anyString())).thenReturn(accountBalance);
        when(paymentRequestProducer.sendPaymentRequest(any(PaymentDto.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // Act
        PaymentDto result = service.processPayment(paymentDto);

        // Assert
        assertEquals(paymentDto, result);

        // Verify interactions
        verify(accountBalanceClient).getBalance(anyString());
        // The producer is called because the currency mismatch exception is caught but not rethrown
        verify(paymentRequestProducer).sendPaymentRequest(paymentDto);

        // Verify that the warning was logged (using LogCaptor)
        assertTrue(logCaptor.getMessages().stream()
                .anyMatch(msg -> msg.contains("Currency mismatch for payment") && 
                                 msg.contains(paymentDto.getId())));
    }

    @Test
    void processPayment_withAccountNotFound_shouldThrowException() {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", "Test payment");

        when(accountBalanceClient.getBalance(anyString())).thenThrow(new AccountNotFoundException("Account not found"));

        // Act & Assert
        AccountNotFoundException exception = assertThrows(AccountNotFoundException.class, 
                () -> service.processPayment(paymentDto));
        assertEquals("Account not found", exception.getMessage());

        // Verify interactions
        verify(accountBalanceClient).getBalance(anyString());
        verifyNoInteractions(paymentRequestProducer);
    }
}
