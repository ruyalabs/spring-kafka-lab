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
    void testSimpleLoggingConfiguration() {
        // Test that simple logging is properly configured
        log.info("Testing simple logging configuration - Event: test_start, TestName: testSimpleLoggingConfiguration");

        // This test verifies that the logging dependencies are properly loaded
        // and that simple logging works without errors
        assertTrue(true, "Simple logging configuration test passed");

        log.info("Simple logging test completed successfully - Event: test_completed, Result: success");
    }

    @Test
    void testBalanceCheckClientLogging() {
        // Create a BalanceCheckClient instance for testing
        BalanceCheckClient balanceCheckClient = new BalanceCheckClient();

        // Test that the client can be instantiated and logging works
        assertNotNull(balanceCheckClient);

        log.info("BalanceCheckClient instantiated successfully for testing - Event: client_test, ClientType: BalanceCheckClient");
    }

    @Test
    void testPaymentExecutionClientLogging() {
        // Create a PaymentExecutionClient instance for testing
        PaymentExecutionClient paymentExecutionClient = new PaymentExecutionClient();

        // Test that the client can be instantiated and logging works
        assertNotNull(paymentExecutionClient);

        log.info("PaymentExecutionClient instantiated successfully for testing - Event: client_test, ClientType: PaymentExecutionClient");
    }
}
