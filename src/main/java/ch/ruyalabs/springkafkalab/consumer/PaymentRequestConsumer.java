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
            } else {
                log.info("Payment skipped due to insufficient balance for customer: {}", paymentDto.getCustomerId());
            }
            // Acknowledge successful processing (including insufficient balance case)
            acknowledgment.acknowledge();
        } catch (BalanceCheckClient.InsufficientBalanceException e) {
            log.error("Payment failed due to insufficient balance: {}", e.getMessage());
            // Acknowledge - business error, no need to retry
            acknowledgment.acknowledge();
        } catch (BalanceCheckClient.AccountNotFoundException e) {
            log.error("Payment failed due to account not found: {}", e.getMessage());
            // Acknowledge - business error, no need to retry
            acknowledgment.acknowledge();
        } catch (BalanceCheckClient.ServiceUnavailableException e) {
            log.error("Payment failed due to balance check service unavailable: {}", e.getMessage());
            // Don't acknowledge - service error, allow retry
        } catch (PaymentExecutionClient.PaymentProcessingException e) {
            log.error("Payment failed during processing: {}", e.getMessage());
            // Acknowledge - business error, no need to retry
            acknowledgment.acknowledge();
        } catch (PaymentExecutionClient.InvalidPaymentMethodException e) {
            log.error("Payment failed due to invalid payment method: {}", e.getMessage());
            // Acknowledge - business error, no need to retry
            acknowledgment.acknowledge();
        } catch (PaymentExecutionClient.GatewayTimeoutException e) {
            log.error("Payment failed due to gateway timeout: {}", e.getMessage());
            // Don't acknowledge - timeout error, allow retry
        } catch (Exception e) {
            log.error("Unexpected error during payment processing: {}", e.getMessage(), e);
            // Acknowledge - prevent infinite retries for unexpected errors
            acknowledgment.acknowledge();
        }
    }
}
