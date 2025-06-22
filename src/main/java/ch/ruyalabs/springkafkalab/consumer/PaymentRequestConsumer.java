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
import org.springframework.transaction.annotation.Transactional;

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
    @Transactional(transactionManager = "chainedKafkaTransactionManager")
    public void consume(@Payload @Valid PaymentDto paymentDto, Acknowledgment acknowledgment) 
            throws Exception {
        log.info("Consumed PaymentDto from payment-request topic: {}", paymentDto);

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
    }
}
