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
    public void consume(@Payload @Valid PaymentDto paymentDto)
            throws Exception {

        log.info("Payment request consumed from Kafka topic: payment-request - PaymentId: {}, CustomerId: {}, Amount: {} {}, Operation: payment_request_processing",
                paymentDto.getPaymentId(), paymentDto.getCustomerId(), paymentDto.getAmount(), paymentDto.getCurrency());

        boolean balanceCheckResult = balanceCheckClient.checkBalance(
                paymentDto.getCustomerId(),
                paymentDto.getAmount()
        );

        if (balanceCheckResult) {
            paymentExecutionClient.executePayment(paymentDto);
            log.info("Payment request processed successfully - Status: success, CustomerId: {}, PaymentId: {}",
                    paymentDto.getCustomerId(), paymentDto.getPaymentId());
            paymentResponseProducer.sendSuccessResponse(paymentDto);
        } else {
            log.info("Payment request skipped due to insufficient balance - Reason: insufficient_balance, CustomerId: {}, PaymentId: {}",
                    paymentDto.getCustomerId(), paymentDto.getPaymentId());
            paymentResponseProducer.sendErrorResponse(paymentDto, "Insufficient balance for payment");
        }
    }
}
