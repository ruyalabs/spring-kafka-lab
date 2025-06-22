import ch.ruyalabs.springkafkalab.client.BalanceCheckClient;
import ch.ruyalabs.springkafkalab.client.PaymentExecutionClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class test_logging_demo {
    private static final Logger log = LoggerFactory.getLogger(test_logging_demo.class);
    
    public static void main(String[] args) {
        log.info("=== LOGGING DEMO: MDC and Structured Arguments Removed ===");
        
        // Demonstrate simple detailed logging
        String paymentId = "PAY-12345";
        String customerId = "CUST-67890";
        Double amount = 100.50;
        String currency = "USD";
        
        log.info("Payment request consumed from Kafka topic: payment-request - PaymentId: {}, CustomerId: {}, Amount: {} {}, Operation: payment_request_processing", 
                paymentId, customerId, amount, currency);
        
        log.info("Starting balance check simulation - Operation: balance_check, CustomerId: {}, RequiredAmount: {}", 
                customerId, amount);
        
        log.info("Balance check completed successfully - BalanceStatus: sufficient, CustomerId: {}", customerId);
        
        log.info("Starting payment execution simulation - Operation: payment_execution, PaymentId: {}, CustomerId: {}, Amount: {} {}, PaymentMethod: {}", 
                paymentId, customerId, amount, currency, "CREDIT_CARD");
        
        log.info("Payment execution completed successfully - Result: Payment executed successfully for ID: {} with amount: {} {}", 
                paymentId, amount, currency);
        
        log.info("Payment request processed successfully - Status: success, CustomerId: {}, PaymentId: {}", 
                customerId, paymentId);
        
        log.info("=== DEMO COMPLETE: All logging now uses simple @Slf4j with detailed string formatting ===");
    }
}