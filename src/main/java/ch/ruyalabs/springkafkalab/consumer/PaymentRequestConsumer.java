package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.client.BalanceCheckClient;
import ch.ruyalabs.springkafkalab.client.PaymentExecutionClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRequestConsumer {

    private final BalanceCheckClient balanceCheckClient;
    private final PaymentExecutionClient paymentExecutionClient;

    @KafkaListener(topics = "${app.kafka.topics.payment-request}", groupId = "${app.kafka.consumer.payment-request.group-id}")
    public void consume(@Payload @Valid PaymentDto paymentDto) {
        log.info("Consumed PaymentDto from payment-request topic: {}", paymentDto);

        try {
            boolean balanceCheckResult = balanceCheckClient.checkBalance(
                    paymentDto.getCustomerId(),
                    paymentDto.getAmount()
            );
            if (balanceCheckResult) {
                paymentExecutionClient.executePayment(paymentDto);
            }
        } catch (BalanceCheckClient.InsufficientBalanceException e) {
            log.error("Payment failed due to insufficient balance: {}", e.getMessage());
        } catch (BalanceCheckClient.AccountNotFoundException e) {
            log.error("Payment failed due to account not found: {}", e.getMessage());
        } catch (BalanceCheckClient.ServiceUnavailableException e) {
            log.error("Payment failed due to balance check service unavailable: {}", e.getMessage());
        } catch (PaymentExecutionClient.PaymentProcessingException e) {
            log.error("Payment failed during processing: {}", e.getMessage());
        } catch (PaymentExecutionClient.InvalidPaymentMethodException e) {
            log.error("Payment failed due to invalid payment method: {}", e.getMessage());
        } catch (PaymentExecutionClient.GatewayTimeoutException e) {
            log.error("Payment failed due to gateway timeout: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error during payment processing: {}", e.getMessage(), e);
        }
    }
}
