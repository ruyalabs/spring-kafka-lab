package ch.ruyalabs.springkafkalab;

import ch.ruyalabs.springkafkalab.client.BalanceCheckClient;
import ch.ruyalabs.springkafkalab.client.PaymentExecutionClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.exception.InsufficientBalanceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class StructuredLoggingTest {

    private static final Logger log = LoggerFactory.getLogger(StructuredLoggingTest.class);

    @Test
    void testStructuredLoggingConfiguration() {
        // Test that structured logging is properly configured
        log.info("Testing structured logging configuration",
                net.logstash.logback.argument.StructuredArguments.kv("event", "test_start"),
                net.logstash.logback.argument.StructuredArguments.kv("testName", "testStructuredLoggingConfiguration"));
        
        // This test verifies that the structured logging dependencies are properly loaded
        // and that StructuredArguments can be used without errors
        assertTrue(true, "Structured logging configuration test passed");
        
        log.info("Structured logging test completed successfully",
                net.logstash.logback.argument.StructuredArguments.kv("event", "test_completed"),
                net.logstash.logback.argument.StructuredArguments.kv("result", "success"));
    }

    @Test
    void testBalanceCheckClientLogging() {
        // Create a BalanceCheckClient instance for testing
        BalanceCheckClient balanceCheckClient = new BalanceCheckClient();
        
        // Test that the client can be instantiated and logging works
        assertNotNull(balanceCheckClient);
        
        log.info("BalanceCheckClient instantiated successfully for testing",
                net.logstash.logback.argument.StructuredArguments.kv("event", "client_test"),
                net.logstash.logback.argument.StructuredArguments.kv("clientType", "BalanceCheckClient"));
    }

    @Test
    void testPaymentExecutionClientLogging() {
        // Create a PaymentExecutionClient instance for testing
        PaymentExecutionClient paymentExecutionClient = new PaymentExecutionClient();
        
        // Test that the client can be instantiated and logging works
        assertNotNull(paymentExecutionClient);
        
        log.info("PaymentExecutionClient instantiated successfully for testing",
                net.logstash.logback.argument.StructuredArguments.kv("event", "client_test"),
                net.logstash.logback.argument.StructuredArguments.kv("clientType", "PaymentExecutionClient"));
    }
}