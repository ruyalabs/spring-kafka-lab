package ch.ruyalabs.springkafkalab;

import ch.ruyalabs.springkafkalab.consumer.PaymentResponseProducer;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(partitions = 1, 
               topics = {"payment-request-test", "payment-response-test"},
               brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@ActiveProfiles("test")
class PaymentErrorRecoveryTest {

    @Autowired
    private PaymentResponseProducer paymentResponseProducer;

    @Test
    void testErrorResponseSendingInTransaction() throws Exception {
        System.out.println("[DEBUG_LOG] Starting error recovery transaction test");

        // Create a test PaymentDto
        PaymentDto paymentDto = new PaymentDto()
                .paymentId("test_payment_123")
                .customerId("test_customer_456")
                .amount(100.00)
                .currency("USD")
                .paymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);

        try {
            System.out.println("[DEBUG_LOG] Sending error response using sendErrorResponse method");

            // This should work without transaction context issues
            paymentResponseProducer.sendErrorResponse(paymentDto, "Test error message for transaction context");

            System.out.println("[DEBUG_LOG] Error response sent successfully");
            System.out.println("[DEBUG_LOG] Test completed successfully - transaction context issue resolved");

        } catch (Exception e) {
            System.out.println("[DEBUG_LOG] Test failed with exception: " + e.getMessage());
            e.printStackTrace();
            fail("Error response sending should work without transaction context issues: " + e.getMessage());
        }
    }
}
