package ch.ruyalabs.springkafkalab.consumer;

import ch.qos.logback.classic.Level;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient;
import ch.ruyalabs.springkafkalab.client.BookingClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.util.LogCaptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.web.client.ResourceAccessException;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentRequestConsumerTest {

    @Mock
    private AccountBalanceClient accountBalanceClient;

    @Mock
    private BookingClient bookingClient;

    @Mock
    private Acknowledgment acknowledgment;

    private PaymentRequestConsumer consumer;
    private LogCaptor logCaptor;

    private static final String PAYMENT_ID = "payment-123";
    private static final String ACCOUNT_ID = "account-" + PAYMENT_ID;
    private static final BigDecimal PAYMENT_AMOUNT = new BigDecimal("100.00");
    private static final String PAYMENT_CURRENCY = "USD";
    private static final String PAYMENT_DESCRIPTION = "Test payment";

    @BeforeEach
    void setUp() {
        consumer = new PaymentRequestConsumer(accountBalanceClient, bookingClient);
        logCaptor = new LogCaptor(PaymentRequestConsumer.class.getName());
    }

    @AfterEach
    void tearDown() {
        logCaptor.stop();
    }

    @Test
    void processPayment_happyPath_shouldAcknowledgeMessage() {
        // Arrange
        PaymentDto payment = new PaymentDto(PAYMENT_ID, PAYMENT_AMOUNT, PAYMENT_CURRENCY, PAYMENT_DESCRIPTION);

        // Mock account balance response with sufficient funds
        AccountBalanceClient.AccountBalance balance = new AccountBalanceClient.AccountBalance(
                ACCOUNT_ID, new BigDecimal("200.00"), PAYMENT_CURRENCY);
        when(accountBalanceClient.getBalance(anyString())).thenReturn(balance);

        // Mock booking response
        BookingClient.BookingResponse bookingResponse = new BookingClient.BookingResponse(
                "booking-456", "Payment: " + PAYMENT_DESCRIPTION, "COMPLETED");
        when(bookingClient.createBooking(any(BookingClient.BookingRequest.class))).thenReturn(bookingResponse);

        // Act
        consumer.processPayment(payment, "key-123", 0, "payment-request", 0L, acknowledgment);

        // Assert
        verify(accountBalanceClient).getBalance(ACCOUNT_ID);
        verify(bookingClient).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Received payment request")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Checking account balance")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Account balance for payment")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Creating booking")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Booking created")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Successfully processed payment")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Acknowledged payment request")));
    }

    @Test
    void processPayment_insufficientFunds_shouldThrowExceptionAndNotAcknowledge() {
        // Arrange
        PaymentDto payment = new PaymentDto(PAYMENT_ID, PAYMENT_AMOUNT, PAYMENT_CURRENCY, PAYMENT_DESCRIPTION);

        // Mock account balance response with insufficient funds
        AccountBalanceClient.AccountBalance balance = new AccountBalanceClient.AccountBalance(
                ACCOUNT_ID, new BigDecimal("50.00"), PAYMENT_CURRENCY);
        when(accountBalanceClient.getBalance(anyString())).thenReturn(balance);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> 
            consumer.processPayment(payment, "key-123", 0, "payment-request", 0L, acknowledgment)
        );

        assertTrue(exception.getMessage().contains("Insufficient funds"));

        verify(accountBalanceClient).getBalance(ACCOUNT_ID);
        verify(bookingClient, never()).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment, never()).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Insufficient funds")));
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Error processing payment request")));
    }

    @Test
    void processPayment_accountBalanceClientFailure_shouldThrowExceptionAndNotAcknowledge() {
        // Arrange
        PaymentDto payment = new PaymentDto(PAYMENT_ID, PAYMENT_AMOUNT, PAYMENT_CURRENCY, PAYMENT_DESCRIPTION);

        // Mock account balance client to throw exception
        ResourceAccessException resourceException = new ResourceAccessException("Service unavailable");
        when(accountBalanceClient.getBalance(anyString())).thenThrow(resourceException);

        // Act & Assert
        Exception exception = assertThrows(ResourceAccessException.class, () -> 
            consumer.processPayment(payment, "key-123", 0, "payment-request", 0L, acknowledgment)
        );

        assertEquals("Service unavailable", exception.getMessage());

        verify(accountBalanceClient).getBalance(ACCOUNT_ID);
        verify(bookingClient, never()).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment, never()).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Error processing payment request")));
    }

    @Test
    void processPayment_bookingClientFailure_shouldThrowExceptionAndNotAcknowledge() {
        // Arrange
        PaymentDto payment = new PaymentDto(PAYMENT_ID, PAYMENT_AMOUNT, PAYMENT_CURRENCY, PAYMENT_DESCRIPTION);

        // Mock account balance response with sufficient funds
        AccountBalanceClient.AccountBalance balance = new AccountBalanceClient.AccountBalance(
                ACCOUNT_ID, new BigDecimal("200.00"), PAYMENT_CURRENCY);
        when(accountBalanceClient.getBalance(anyString())).thenReturn(balance);

        // Mock booking client to throw exception
        ResourceAccessException resourceException = new ResourceAccessException("Service unavailable");
        when(bookingClient.createBooking(any(BookingClient.BookingRequest.class))).thenThrow(resourceException);

        // Act & Assert
        Exception exception = assertThrows(ResourceAccessException.class, () -> 
            consumer.processPayment(payment, "key-123", 0, "payment-request", 0L, acknowledgment)
        );

        assertEquals("Service unavailable", exception.getMessage());

        verify(accountBalanceClient).getBalance(ACCOUNT_ID);
        verify(bookingClient).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment, never()).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Error processing payment request")));
    }
}
