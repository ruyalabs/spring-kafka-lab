package ch.ruyalabs.springkafkalab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class KafkaTopicConfigTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private KafkaTopicConfig kafkaTopicConfig;

    @Test
    void testPaymentRequestTopicBeanExists() {
        // Verify that the paymentRequestTopic bean exists in the application context
        assertTrue(applicationContext.containsBean("paymentRequestTopic"));
    }

    @Test
    void testPaymentRequestTopicConfiguration() {
        // Get the NewTopic bean from the configuration
        NewTopic paymentRequestTopic = kafkaTopicConfig.paymentRequestTopic();

        // Verify the topic configuration
        assertNotNull(paymentRequestTopic, "PaymentRequestTopic should not be null");
        assertEquals("payment-request", paymentRequestTopic.name(), "Topic name should be 'payment-request'");
        assertEquals(3, paymentRequestTopic.numPartitions(), "Topic should have 3 partitions");
        assertEquals((short) 3, paymentRequestTopic.replicationFactor(), "Topic should have replication factor of 3");
    }

    @Test
    void testPaymentRequestTopicFromApplicationContext() {
        // Get the NewTopic bean directly from the application context
        NewTopic paymentRequestTopic = applicationContext.getBean("paymentRequestTopic", NewTopic.class);

        // Verify the topic configuration
        assertNotNull(paymentRequestTopic, "PaymentRequestTopic bean should not be null");
        assertEquals("payment-request", paymentRequestTopic.name(), "Topic name should be 'payment-request'");
        assertEquals(3, paymentRequestTopic.numPartitions(), "Topic should have 3 partitions");
        assertEquals((short) 3, paymentRequestTopic.replicationFactor(), "Topic should have replication factor of 3");
    }

    @Test
    void testKafkaTopicConfigIsConfiguration() {
        // Verify that KafkaTopicConfig is properly loaded as a Spring configuration
        // We check this by verifying that the configuration class name contains our expected class
        // This works even with Spring proxies
        String className = kafkaTopicConfig.getClass().getName();
        assertTrue(className.contains("KafkaTopicConfig"), 
                "KafkaTopicConfig should be loaded as a Spring configuration bean");
    }
}
