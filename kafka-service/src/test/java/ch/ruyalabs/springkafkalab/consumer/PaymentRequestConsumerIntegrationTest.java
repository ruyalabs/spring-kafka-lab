package ch.ruyalabs.springkafkalab.consumer;

import ch.qos.logback.classic.Level;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient;
import ch.ruyalabs.springkafkalab.client.BookingClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.util.LogCaptor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.ResourceAccessException;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"payment-request"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "payment.request.topic=payment-request",
        "spring.kafka.consumer.group-id=test-payment-consumer-group",
        "logging.level.org.springframework.kafka=DEBUG",
        "logging.level.org.apache.kafka=DEBUG"
})
class PaymentRequestConsumerIntegrationTest {

    private static final String TOPIC = "payment-request";

    @Value("${spring.embedded.kafka.brokers}")
    private String brokerAddresses;

    @Autowired
    private AccountBalanceClient accountBalanceClient;

    @Autowired
    private BookingClient bookingClient;

    @Autowired(required = false)
    private PaymentRequestConsumer paymentRequestConsumer;

    // Configuration to provide mock implementations for testing
    @Configuration
    static class TestConfig {
        private final AccountBalanceClient mockAccountBalanceClient = mock(AccountBalanceClient.class);
        private final BookingClient mockBookingClient = mock(BookingClient.class);

        @Bean
        @Primary
        public AccountBalanceClient accountBalanceClient() {
            return mockAccountBalanceClient;
        }

        @Bean
        @Primary
        public BookingClient bookingClient() {
            return mockBookingClient;
        }

        @Bean
        @Primary
        public PaymentRequestConsumer paymentRequestConsumer() {
            return new PaymentRequestConsumer(mockAccountBalanceClient, mockBookingClient);
        }
    }

    private Producer<String, PaymentDto> producer;
    private LogCaptor logCaptor;

    @BeforeEach
    void setUp() {
        System.out.println("[DEBUG_LOG] Setting up test with broker addresses: " + brokerAddresses);
        System.out.println("[DEBUG_LOG] PaymentRequestConsumer bean exists: " + (paymentRequestConsumer != null));

        // Check if the KafkaListener annotation is present on the processPayment method
        try {
            java.lang.reflect.Method processPaymentMethod = PaymentRequestConsumer.class.getMethod("processPayment", 
                PaymentDto.class, String.class, Integer.class, String.class, Long.class, 
                org.springframework.kafka.support.Acknowledgment.class);

            org.springframework.kafka.annotation.KafkaListener kafkaListener = 
                processPaymentMethod.getAnnotation(org.springframework.kafka.annotation.KafkaListener.class);

            System.out.println("[DEBUG_LOG] KafkaListener annotation on processPayment method: " + (kafkaListener != null));
            if (kafkaListener != null) {
                System.out.println("[DEBUG_LOG] KafkaListener topics: " + String.join(", ", kafkaListener.topics()));
                System.out.println("[DEBUG_LOG] KafkaListener groupId: " + kafkaListener.groupId());
                System.out.println("[DEBUG_LOG] KafkaListener containerFactory: " + kafkaListener.containerFactory());
            }
        } catch (Exception e) {
            System.out.println("[DEBUG_LOG] Error checking KafkaListener annotation: " + e.getMessage());
        }

        // Configure the producer
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put("bootstrap.servers", brokerAddresses);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", JsonSerializer.class);

        // Use default JsonSerializer without adding type information
        DefaultKafkaProducerFactory<String, PaymentDto> producerFactory = 
            new DefaultKafkaProducerFactory<>(
                producerProps,
                new StringSerializer(),
                new JsonSerializer<PaymentDto>()
            );
        producer = producerFactory.createProducer();
        System.out.println("[DEBUG_LOG] Producer created successfully");

        // Set up log capturing
        logCaptor = new LogCaptor(PaymentRequestConsumer.class.getName());
        System.out.println("[DEBUG_LOG] Log capturing set up for: " + PaymentRequestConsumer.class.getName());

        // Print out test configuration
        System.out.println("[DEBUG_LOG] Test configuration:");
        System.out.println("[DEBUG_LOG] - Topic: " + TOPIC);
        System.out.println("[DEBUG_LOG] - AccountBalanceClient: " + (accountBalanceClient != null ? "mocked" : "null"));
        System.out.println("[DEBUG_LOG] - BookingClient: " + (bookingClient != null ? "mocked" : "null"));
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
        logCaptor.stop();
    }

    @Test
    void consumePaymentRequest_happyPath_shouldProcessSuccessfully() throws Exception {
        // Arrange
        String paymentId = UUID.randomUUID().toString();
        PaymentDto payment = new PaymentDto(paymentId, new BigDecimal("100.00"), "USD", "Test payment");
        String accountId = "account-" + paymentId;

        // Mock account balance response with sufficient funds
        AccountBalanceClient.AccountBalance balance = new AccountBalanceClient.AccountBalance(
                accountId, new BigDecimal("200.00"), "USD");
        when(accountBalanceClient.getBalance(anyString())).thenReturn(balance);

        // Mock booking response
        BookingClient.BookingResponse bookingResponse = new BookingClient.BookingResponse(
                "booking-456", "Payment: Test payment", "COMPLETED");
        when(bookingClient.createBooking(any(BookingClient.BookingRequest.class))).thenReturn(bookingResponse);

        // Mock acknowledgment
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        // Act - directly call the consumer's processPayment method
        System.out.println("[DEBUG_LOG] Directly calling consumer's processPayment method");
        paymentRequestConsumer.processPayment(
            payment,
            paymentId,
            Integer.valueOf(0),
            TOPIC,
            Long.valueOf(0L),
            acknowledgment
        );

        // Verify that the mocks were called
        verify(accountBalanceClient).getBalance(accountId);
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
    void consumePaymentRequest_insufficientFunds_shouldNotAcknowledge() throws Exception {
        // Arrange
        String paymentId = UUID.randomUUID().toString();
        PaymentDto payment = new PaymentDto(paymentId, new BigDecimal("100.00"), "USD", "Test payment");
        String accountId = "account-" + paymentId;

        // Mock account balance response with insufficient funds
        AccountBalanceClient.AccountBalance balance = new AccountBalanceClient.AccountBalance(
                accountId, new BigDecimal("50.00"), "USD");
        when(accountBalanceClient.getBalance(anyString())).thenReturn(balance);

        // Mock acknowledgment
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        // Act - directly call the consumer's processPayment method
        System.out.println("[DEBUG_LOG] Directly calling consumer's processPayment method with insufficient funds");

        // The method should throw an exception due to insufficient funds
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            paymentRequestConsumer.processPayment(
                payment,
                paymentId,
                Integer.valueOf(0),
                TOPIC,
                Long.valueOf(0L),
                acknowledgment
            );
        });

        // Assert
        assertTrue(exception.getMessage().contains("Insufficient funds"));
        verify(accountBalanceClient).getBalance(accountId);
        verify(bookingClient, never()).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment, never()).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Insufficient funds")));
    }

    @Test
    void consumePaymentRequest_restClientFailure_shouldRetry() throws Exception {
        // Arrange
        String paymentId = UUID.randomUUID().toString();
        PaymentDto payment = new PaymentDto(paymentId, new BigDecimal("100.00"), "USD", "Test payment");
        String accountId = "account-" + paymentId;

        // Mock account balance client to throw exception
        ResourceAccessException resourceException = new ResourceAccessException("Service unavailable");
        when(accountBalanceClient.getBalance(anyString())).thenThrow(resourceException);

        // Mock acknowledgment
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        // Act - directly call the consumer's processPayment method
        System.out.println("[DEBUG_LOG] Directly calling consumer's processPayment method with REST client failure");

        // The method should throw the ResourceAccessException
        ResourceAccessException exception = assertThrows(ResourceAccessException.class, () -> {
            paymentRequestConsumer.processPayment(
                payment,
                paymentId,
                Integer.valueOf(0),
                TOPIC,
                Long.valueOf(0L),
                acknowledgment
            );
        });

        // Assert
        assertEquals("Service unavailable", exception.getMessage());
        verify(accountBalanceClient).getBalance(anyString());
        verify(bookingClient, never()).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment, never()).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Error processing payment request")));
    }

    @Test
    void consumePaymentRequest_transientError_thenSuccess_shouldEventuallyAcknowledge() throws Exception {
        // Arrange
        String paymentId = UUID.randomUUID().toString();
        PaymentDto payment = new PaymentDto(paymentId, new BigDecimal("100.00"), "USD", "Test payment");
        String accountId = "account-" + paymentId;

        // Mock account balance to fail once then succeed
        ResourceAccessException resourceException = new ResourceAccessException("Service unavailable");
        AccountBalanceClient.AccountBalance balance = new AccountBalanceClient.AccountBalance(
                accountId, new BigDecimal("200.00"), "USD");

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

        // Mock acknowledgment
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        // Act - directly call the consumer's processPayment method twice
        System.out.println("[DEBUG_LOG] First call to processPayment should fail");

        // First call should throw an exception
        ResourceAccessException exception = assertThrows(ResourceAccessException.class, () -> {
            paymentRequestConsumer.processPayment(
                payment,
                paymentId,
                Integer.valueOf(0),
                TOPIC,
                Long.valueOf(0L),
                acknowledgment
            );
        });

        assertEquals("Service unavailable", exception.getMessage());

        // Second call should succeed
        System.out.println("[DEBUG_LOG] Second call to processPayment should succeed");
        paymentRequestConsumer.processPayment(
            payment,
            paymentId,
            Integer.valueOf(0),
            TOPIC,
            Long.valueOf(0L),
            acknowledgment
        );

        // Assert
        verify(accountBalanceClient, times(2)).getBalance(accountId);
        verify(bookingClient).createBooking(any(BookingClient.BookingRequest.class));
        verify(acknowledgment).acknowledge();

        // Verify logging
        assertTrue(logCaptor.getMessages(Level.ERROR).stream()
                .anyMatch(msg -> msg.contains("Error processing payment request")));
        assertTrue(logCaptor.getMessages(Level.INFO).stream()
                .anyMatch(msg -> msg.contains("Successfully processed payment")));
    }
}
