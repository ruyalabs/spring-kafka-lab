package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.client.BalanceCheckClient;
import ch.ruyalabs.springkafkalab.client.PaymentExecutionClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRequestConsumer {

    private final BalanceCheckClient balanceCheckClient;
    private final PaymentExecutionClient paymentExecutionClient;
    private final PaymentResponseProducer paymentResponseProducer;

    @KafkaListener(
            topics = "${app.kafka.topics.payment-request}", 
            containerFactory = "paymentRequestKafkaListenerContainerFactory"
    )
    public void consume(@Payload @Valid PaymentDto paymentDto, Acknowledgment acknowledgment) {
        log.info("Consumed PaymentDto from payment-request topic: {}", paymentDto);

        try {
            boolean balanceCheckResult = balanceCheckClient.checkBalance(
                    paymentDto.getCustomerId(),
                    paymentDto.getAmount()
            );
            if (balanceCheckResult) {
                paymentExecutionClient.executePayment(paymentDto);
                log.info("Payment processed successfully for customer: {}", paymentDto.getCustomerId());
                // Send success response
                paymentResponseProducer.sendSuccessResponse(paymentDto);
            } else {
                log.info("Payment skipped due to insufficient balance for customer: {}", paymentDto.getCustomerId());
                // Send error response for insufficient balance
                paymentResponseProducer.sendErrorResponse(paymentDto, "Insufficient balance for payment");
            }
            // Acknowledge successful processing (including insufficient balance case)
            acknowledgment.acknowledge();
        } catch (BalanceCheckClient.InsufficientBalanceException e) {
            log.error("Payment failed due to insufficient balance: {}", e.getMessage());
            // Send error response and acknowledge - business error, no need to retry
            paymentResponseProducer.sendErrorResponse(paymentDto, "Insufficient balance: " + e.getMessage());
            acknowledgment.acknowledge();
        } catch (BalanceCheckClient.AccountNotFoundException e) {
            log.error("Payment failed due to account not found: {}", e.getMessage());
            // Send error response and acknowledge - business error, no need to retry
            paymentResponseProducer.sendErrorResponse(paymentDto, "Account not found: " + e.getMessage());
            acknowledgment.acknowledge();
        } catch (BalanceCheckClient.ServiceUnavailableException e) {
            log.error("Payment failed due to balance check service unavailable: {}", e.getMessage());
            // Don't acknowledge - service error, allow retry (error handler will send response after retries)
        } catch (PaymentExecutionClient.PaymentProcessingException e) {
            log.error("Payment failed during processing: {}", e.getMessage());
            // Send error response and acknowledge - business error, no need to retry
            paymentResponseProducer.sendErrorResponse(paymentDto, "Payment processing failed: " + e.getMessage());
            acknowledgment.acknowledge();
        } catch (PaymentExecutionClient.InvalidPaymentMethodException e) {
            log.error("Payment failed due to invalid payment method: {}", e.getMessage());
            // Send error response and acknowledge - business error, no need to retry
            paymentResponseProducer.sendErrorResponse(paymentDto, "Invalid payment method: " + e.getMessage());
            acknowledgment.acknowledge();
        } catch (PaymentExecutionClient.GatewayTimeoutException e) {
            log.error("Payment failed due to gateway timeout: {}", e.getMessage());
            // Don't acknowledge - timeout error, allow retry (error handler will send response after retries)
        } catch (Exception e) {
            log.error("Unexpected error during payment processing: {}", e.getMessage(), e);
            // Send error response and acknowledge - prevent infinite retries for unexpected errors
            paymentResponseProducer.sendErrorResponse(paymentDto, "Unexpected error: " + e.getMessage());
            acknowledgment.acknowledge();
        }
    }
}
