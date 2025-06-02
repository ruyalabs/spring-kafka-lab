package ch.ruyalabs.springkafkalab.producer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"payment-request-test"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "payment.request.topic=payment-request-test"
})
class PaymentRequestProducerIntegrationTest {

    private static final String TOPIC = "payment-request-test";

    @Value("${spring.embedded.kafka.brokers}")
    private String brokerAddresses;

    @Autowired
    private PaymentRequestProducer producer;

    private Consumer<String, PaymentDto> consumer;

    @BeforeEach
    void setUp() {
        // Configure the consumer
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddresses);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-payment-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        // Create the consumer factory and consumer
        DefaultKafkaConsumerFactory<String, PaymentDto> consumerFactory = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new JsonDeserializer<>(PaymentDto.class, false));

        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC));

        // Clear any existing messages
        consumer.poll(Duration.ofMillis(100));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void sendPaymentRequest_shouldDeliverMessageToKafka() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange
        String paymentId = UUID.randomUUID().toString();
        PaymentDto payment = new PaymentDto(paymentId, new BigDecimal("150.75"), "EUR", "Integration test payment");

        // Act
        producer.sendPaymentRequest(payment).get(10, TimeUnit.SECONDS);

        // Assert
        ConsumerRecord<String, PaymentDto> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofMillis(5000));

        assertNotNull(record);
        assertEquals(paymentId, record.key());

        PaymentDto receivedPayment = record.value();
        assertNotNull(receivedPayment);
        assertEquals(paymentId, receivedPayment.getId());
        assertEquals(0, new BigDecimal("150.75").compareTo(receivedPayment.getAmount()));
        assertEquals("EUR", receivedPayment.getCurrency());
        assertEquals("Integration test payment", receivedPayment.getDescription());
    }

    @Test
    void sendPaymentRequest_withNullId_shouldGenerateIdAndDeliverMessageToKafka() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange
        PaymentDto payment = new PaymentDto(null, new BigDecimal("200.00"), "USD", "Payment with null ID");

        // Act
        producer.sendPaymentRequest(payment).get(10, TimeUnit.SECONDS);

        // Assert
        ConsumerRecord<String, PaymentDto> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofMillis(5000));

        assertNotNull(record);
        String key = record.key();
        assertNotNull(key);
        // Verify it's a valid UUID
        assertDoesNotThrow(() -> UUID.fromString(key));

        PaymentDto receivedPayment = record.value();
        assertNotNull(receivedPayment);
        assertNull(receivedPayment.getId()); // ID in the payload should still be null
        assertEquals(0, new BigDecimal("200.00").compareTo(receivedPayment.getAmount()));
        assertEquals("USD", receivedPayment.getCurrency());
        assertEquals("Payment with null ID", receivedPayment.getDescription());
    }
}
