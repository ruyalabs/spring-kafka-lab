package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.client.BalanceCheckClient;
import ch.ruyalabs.springkafkalab.client.PaymentExecutionClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.exception.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.exception.InsufficientBalanceException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
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
    @Transactional(transactionManager = "kafkaTransactionManager", rollbackFor = Exception.class)
    public void consume(@Payload @Valid PaymentDto paymentDto) throws Exception {

        log.info("Payment request consumed from Kafka topic: payment-request - PaymentId: {}, CustomerId: {}, Amount: {} {}, Operation: payment_request_processing",
                paymentDto.getPaymentId(), paymentDto.getCustomerId(), paymentDto.getAmount(), paymentDto.getCurrency());

        try {
            boolean balanceCheckResult = balanceCheckClient.checkBalance(
                    paymentDto.getCustomerId(),
                    paymentDto.getAmount()
            );

            if (balanceCheckResult) {
                // Add to pending payments first (within transaction) to ensure atomic state update
                PaymentExecutionStatusConsumer.addPendingPayment(paymentDto.getPaymentId(), paymentDto);

                // Then make external call - if this fails, transaction will rollback and state won't be persisted
                paymentExecutionClient.requestPaymentExecution(paymentDto);

                log.info("Payment execution requested successfully - PaymentId: {}, CustomerId: {}, Status: pending_execution",
                        paymentDto.getPaymentId(), paymentDto.getCustomerId());
            } else {
                log.info("Payment request skipped due to insufficient balance - Reason: insufficient_balance, CustomerId: {}, PaymentId: {}",
                        paymentDto.getCustomerId(), paymentDto.getPaymentId());
                paymentResponseProducer.sendErrorResponse(paymentDto, "Insufficient balance for payment");
            }
        } catch (InsufficientBalanceException e) {
            log.warn("Payment request failed due to insufficient balance - PaymentId: {}, CustomerId: {}, ErrorMessage: {}",
                    paymentDto.getPaymentId(), paymentDto.getCustomerId(), e.getMessage());
            paymentResponseProducer.sendErrorResponse(paymentDto, e.getMessage());
        } catch (AccountNotFoundException e) {
            log.error("Payment request failed due to account not found - PaymentId: {}, CustomerId: {}, ErrorMessage: {}",
                    paymentDto.getPaymentId(), paymentDto.getCustomerId(), e.getMessage());
            paymentResponseProducer.sendErrorResponse(paymentDto, e.getMessage());
        }
    }

}
