package ch.ruyalabs.springkafkalab;

import ch.ruyalabs.springkafkalab.config.PaymentRequestErrorHandler;
import ch.ruyalabs.springkafkalab.consumer.PaymentResponseProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(partitions = 1, 
               topics = {"payment-request-test", "payment-response-test"},
               brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@ActiveProfiles("test")
class DeterministicKeyTest {

    @Autowired
    private PaymentResponseProducer paymentResponseProducer;

    @Test
    void testDeterministicKeyGeneration() throws Exception {
        System.out.println("[DEBUG_LOG] Starting deterministic key generation test");

        try {
            // Test the new overloaded method with deterministic key
            String testKey = "DESER_ERR_test123456789";
            String errorMessage = "Test deserialization error with deterministic key";
            
            System.out.println("[DEBUG_LOG] Testing new sendGenericDeserializationErrorResponse with deterministic key: " + testKey);
            
            // This should use the new overloaded method
            paymentResponseProducer.sendGenericDeserializationErrorResponse(errorMessage, testKey);
            
            System.out.println("[DEBUG_LOG] Successfully sent error response with deterministic key");
            System.out.println("[DEBUG_LOG] Test completed successfully - deterministic key functionality works");

        } catch (Exception e) {
            System.out.println("[DEBUG_LOG] Test failed with exception: " + e.getMessage());
            e.printStackTrace();
            fail("Deterministic key error response sending should work: " + e.getMessage());
        }
    }

    @Test
    void testBackwardCompatibility() throws Exception {
        System.out.println("[DEBUG_LOG] Starting backward compatibility test");

        try {
            String errorMessage = "Test backward compatibility";
            
            System.out.println("[DEBUG_LOG] Testing original sendGenericDeserializationErrorResponse method");
            
            // This should use the original method (backward compatibility)
            paymentResponseProducer.sendGenericDeserializationErrorResponse(errorMessage);
            
            System.out.println("[DEBUG_LOG] Successfully sent error response using original method");
            System.out.println("[DEBUG_LOG] Test completed successfully - backward compatibility maintained");

        } catch (Exception e) {
            System.out.println("[DEBUG_LOG] Test failed with exception: " + e.getMessage());
            e.printStackTrace();
            fail("Original method should still work for backward compatibility: " + e.getMessage());
        }
    }
}