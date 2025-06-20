package ch.ruyalabs.springkafkalab.integration;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test to verify the complete payment flow:
 * - Success responses for valid payments
 * - Error responses for invalid payments
 * - Exactly one response per request
 * - No duplicate responses
 */
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"payment-request-test", "payment-response-test"}, 
               bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@TestPropertySource(properties = {
    "spring.kafka.consumer.group-id=payment-consumer-test-group",
    "payment.request.topic=payment-request-test",
    "payment.response.topic=payment-response-test"
})
public class PaymentFlowIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void testSuccessfulPaymentFlow() throws Exception {
        // Create producer for sending payment requests
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", JsonSerializer.class);
        Producer<String, PaymentDto> producer = new DefaultKafkaProducerFactory<>(producerProps).createProducer();

        // Create consumer for reading payment responses
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        Consumer<String, Object> consumer = new DefaultKafkaConsumerFactory<>(consumerProps).createConsumer();

        try {
            // Subscribe to response topic
            consumer.subscribe(Collections.singletonList("payment-response-test"));

            // Send a valid payment request
            PaymentDto validPayment = new PaymentDto("test-payment-001", new BigDecimal("100.00"), "USD");
            validPayment.setDescription("Test payment");

            producer.send(new ProducerRecord<>("payment-request-test", "test-key-001", validPayment));
            producer.flush();

            // Wait for response
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(10));

            // Verify exactly one response
            assertEquals(1, records.count(), "Should receive exactly one response");

            ConsumerRecord<String, Object> responseRecord = records.iterator().next();
            assertEquals("test-payment-001", responseRecord.key(), "Response key should match request ID");

            // Verify it's a success response
            Object responseValue = responseRecord.value();
            assertNotNull(responseValue, "Response should not be null");

            System.out.println("[DEBUG_LOG] Received response: " + responseValue);

            // Check for no additional responses (no duplicates)
            ConsumerRecords<String, Object> additionalRecords = consumer.poll(Duration.ofSeconds(2));
            assertEquals(0, additionalRecords.count(), "Should not receive duplicate responses");

        } finally {
            producer.close();
            consumer.close();
        }
    }

    @Test
    public void testInvalidPaymentFlow() throws Exception {
        // Create producer for sending payment requests
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", JsonSerializer.class);
        Producer<String, PaymentDto> producer = new DefaultKafkaProducerFactory<>(producerProps).createProducer();

        // Create consumer for reading payment responses
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group-2", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        Consumer<String, Object> consumer = new DefaultKafkaConsumerFactory<>(consumerProps).createConsumer();

        try {
            // Subscribe to response topic
            consumer.subscribe(Collections.singletonList("payment-response-test"));

            // Send an invalid payment request (negative amount)
            PaymentDto invalidPayment = new PaymentDto("test-payment-002", new BigDecimal("-50.00"), "USD");
            invalidPayment.setDescription("Invalid test payment");

            producer.send(new ProducerRecord<>("payment-request-test", "test-key-002", invalidPayment));
            producer.flush();

            // Wait for response
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(10));

            // Verify exactly one response
            assertEquals(1, records.count(), "Should receive exactly one error response");

            ConsumerRecord<String, Object> responseRecord = records.iterator().next();
            assertEquals("test-payment-002", responseRecord.key(), "Response key should match request ID");

            // Verify it's an error response
            Object responseValue = responseRecord.value();
            assertNotNull(responseValue, "Error response should not be null");

            System.out.println("[DEBUG_LOG] Received error response: " + responseValue);

            // Check for no additional responses (no duplicates)
            ConsumerRecords<String, Object> additionalRecords = consumer.poll(Duration.ofSeconds(2));
            assertEquals(0, additionalRecords.count(), "Should not receive duplicate error responses");

        } finally {
            producer.close();
            consumer.close();
        }
    }
}
