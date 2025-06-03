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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Direct tests for PaymentRequestConsumer without using Spring context.
 * This allows us to test the consumer's logic directly without the complexity
 * of Kafka integration.
 */
@ExtendWith(MockitoExtension.class)
class PaymentRequestConsumerDirectTest {

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
    private static final String TOPIC = "payment-request";

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

        // Act - directly call the consumer's processPayment method
        System.out.println("[DEBUG_LOG] Directly calling consumer's processPayment method");
        consumer.processPayment(
            payment,
            PAYMENT_ID,
            Integer.valueOf(0),
            TOPIC,
            Long.valueOf(0L),
            acknowledgment
        );

        // Assert
        verify(accountBalanceClient).getBalance(ACCOUNT_ID);
        verify(bookingClient).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Received payment request")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Successfully processed payment")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Acknowledged payment request")));
    }

    @Test
    void processPayment_insufficientFunds_shouldNotAcknowledge() {
        // Arrange
        PaymentDto payment = new PaymentDto(PAYMENT_ID, PAYMENT_AMOUNT, PAYMENT_CURRENCY, PAYMENT_DESCRIPTION);

        // Mock account balance response with insufficient funds
        AccountBalanceClient.AccountBalance balance = new AccountBalanceClient.AccountBalance(
                ACCOUNT_ID, new BigDecimal("50.00"), PAYMENT_CURRENCY);
        when(accountBalanceClient.getBalance(anyString())).thenReturn(balance);

        // Act - directly call the consumer's processPayment method
        System.out.println("[DEBUG_LOG] Directly calling consumer's processPayment method with insufficient funds");
        
        // The method should throw an exception due to insufficient funds
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            consumer.processPayment(
                payment,
                PAYMENT_ID,
                Integer.valueOf(0),
                TOPIC,
                Long.valueOf(0L),
                acknowledgment
            );
        });

        // Assert
        assertTrue(exception.getMessage().contains("Insufficient funds"));
        verify(accountBalanceClient).getBalance(ACCOUNT_ID);
        verify(bookingClient, never()).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment, never()).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Insufficient funds")));
    }

    @Test
    void processPayment_restClientFailure_shouldRetry() {
        // Arrange
        PaymentDto payment = new PaymentDto(PAYMENT_ID, PAYMENT_AMOUNT, PAYMENT_CURRENCY, PAYMENT_DESCRIPTION);

        // Mock account balance client to throw exception
        ResourceAccessException resourceException = new ResourceAccessException("Service unavailable");
        when(accountBalanceClient.getBalance(anyString())).thenThrow(resourceException);

        // Act - directly call the consumer's processPayment method
        System.out.println("[DEBUG_LOG] Directly calling consumer's processPayment method with REST client failure");
        
        // The method should throw the ResourceAccessException
        ResourceAccessException exception = assertThrows(ResourceAccessException.class, () -> {
            consumer.processPayment(
                payment,
                PAYMENT_ID,
                Integer.valueOf(0),
                TOPIC,
                Long.valueOf(0L),
                acknowledgment
            );
        });

        // Assert
        assertEquals("Service unavailable", exception.getMessage());
        verify(accountBalanceClient).getBalance(ACCOUNT_ID);
        verify(bookingClient, never()).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment, never()).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Error processing payment request")));
    }

    @Test
    void processPayment_transientError_thenSuccess_shouldEventuallyAcknowledge() {
        // Arrange
        PaymentDto payment = new PaymentDto(PAYMENT_ID, PAYMENT_AMOUNT, PAYMENT_CURRENCY, PAYMENT_DESCRIPTION);

        // Mock account balance to fail once then succeed
        ResourceAccessException resourceException = new ResourceAccessException("Service unavailable");
        AccountBalanceClient.AccountBalance balance = new AccountBalanceClient.AccountBalance(
                ACCOUNT_ID, new BigDecimal("200.00"), PAYMENT_CURRENCY);

        // Use AtomicInteger to track the number of calls to getBalance
        java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger(0);

        when(accountBalanceClient.getBalance(anyString())).thenAnswer(invocation -> {
            System.out.println("[DEBUG_LOG] AccountBalanceClient.getBalance called with accountId: " + invocation.getArgument(0));
            if (callCount.getAndIncrement() == 0) {
                System.out.println("[DEBUG_LOG] First call to getBalance, throwing exception");
                throw resourceException;
            } else {
                System.out.println("[DEBUG_LOG] Subsequent call to getBalance, returning balance");
                return balance;
            }
        });

        // Mock booking response
        BookingClient.BookingResponse bookingResponse = new BookingClient.BookingResponse(
                "booking-456", "Payment: Test payment", "COMPLETED");
        when(bookingClient.createBooking(any(BookingClient.BookingRequest.class))).thenReturn(bookingResponse);

        // Act - directly call the consumer's processPayment method twice
        System.out.println("[DEBUG_LOG] First call to processPayment should fail");
        
        // First call should throw an exception
        ResourceAccessException exception = assertThrows(ResourceAccessException.class, () -> {
            consumer.processPayment(
                payment,
                PAYMENT_ID,
                Integer.valueOf(0),
                TOPIC,
                Long.valueOf(0L),
                acknowledgment
            );
        });
        
        assertEquals("Service unavailable", exception.getMessage());
        
        // Second call should succeed
        System.out.println("[DEBUG_LOG] Second call to processPayment should succeed");
        consumer.processPayment(
            payment,
            PAYMENT_ID,
            Integer.valueOf(0),
            TOPIC,
            Long.valueOf(0L),
            acknowledgment
        );

        // Assert
        verify(accountBalanceClient, times(2)).getBalance(ACCOUNT_ID);
        verify(bookingClient).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Error processing payment request")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Successfully processed payment")));
    }
}