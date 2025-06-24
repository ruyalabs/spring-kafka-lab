package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.client.BalanceCheckClient;
import ch.ruyalabs.springkafkalab.client.PaymentExecutionClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
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
    public void consume(@Payload @Valid PaymentDto paymentDto,
                        @Header(value = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer deliveryAttempt) throws Exception {

        log.info("Payment request consumed from Kafka topic: payment-request - PaymentId: {}, CustomerId: {}, Amount: {} {}, DeliveryAttempt: {}, Operation: payment_request_processing",
                paymentDto.getPaymentId(), paymentDto.getCustomerId(), paymentDto.getAmount(), paymentDto.getCurrency(), deliveryAttempt);

        if (deliveryAttempt != null && deliveryAttempt > 3) {
            log.warn("Poison pill detected - message has been retried {} times - PaymentId: {}, CustomerId: {}",
                    deliveryAttempt, paymentDto.getPaymentId(), paymentDto.getCustomerId());

            try {
                String poisonPillErrorMessage = "Payment processing failed after multiple attempts. Message marked as poison pill.";
                paymentResponseProducer.sendErrorResponse(paymentDto, poisonPillErrorMessage);
                log.info("Poison pill error response sent successfully - PaymentId: {}, CustomerId: {}",
                        paymentDto.getPaymentId(), paymentDto.getCustomerId());
            } catch (Exception e) {
                log.error("CRITICAL: Failed to send poison pill error response - PaymentId: {}, CustomerId: {}, ErrorType: {}, ErrorMessage: {}. " +
                        "This violates the exactly-one-response guarantee and requires manual intervention.",
                        paymentDto.getPaymentId(), paymentDto.getCustomerId(), e.getClass().getSimpleName(), e.getMessage(), e);

                log.warn("Proceeding with transaction commit despite poison pill response failure to unblock partition - PaymentId: {}, CustomerId: {}. " +
                        "MANUAL ACTION REQUIRED: Verify if response was sent and take corrective action if needed.",
                        paymentDto.getPaymentId(), paymentDto.getCustomerId());
            }
            return;
        }

        try {
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
        } catch (Exception e) {
            log.error("Exception occurred while processing payment request - PaymentId: {}, CustomerId: {}, ErrorType: {}, ErrorMessage: {}",
                    paymentDto.getPaymentId(), paymentDto.getCustomerId(), e.getClass().getSimpleName(), e.getMessage(), e);
            String errorMessage = buildErrorMessage(e);
            paymentResponseProducer.sendErrorResponse(paymentDto, errorMessage);
            log.info("Error response sent successfully for failed payment - PaymentId: {}, CustomerId: {}",
                    paymentDto.getPaymentId(), paymentDto.getCustomerId());
        }
    }

    private String buildErrorMessage(Exception exception) {
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append("Payment processing failed. ");
        errorMsg.append("Error: ").append(exception.getMessage());

        if (exception.getCause() != null) {
            errorMsg.append(" Cause: ").append(exception.getCause().getMessage());
        }

        return errorMsg.toString();
    }
}
