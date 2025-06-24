package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.client.BalanceCheckClient;
import ch.ruyalabs.springkafkalab.client.PaymentExecutionClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.exception.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.exception.InsufficientBalanceException;
import ch.ruyalabs.springkafkalab.exception.ServiceUnavailableException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Payment Request Consumer
 * 
 * This consumer implements a strict "no retries" policy to meet the following requirements:
 * - All listeners must not implement any retries
 * - No partition should ever be blocked (resilience to poison pills)
 * - Exactly one response per request
 * - No request may go unanswered
 * 
 * IMPORTANT BUSINESS IMPLICATIONS:
 * - Temporary service failures (e.g., ServiceUnavailableException) result in immediate
 *   and permanent payment failures without retry attempts
 * - Business stakeholders should be aware that temporary external service outages
 *   will result in permanent payment failures
 * - This is the only possible implementation given the "no retries" constraint
 */
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
                PaymentExecutionStatusConsumer.addPendingPayment(paymentDto.getPaymentId(), paymentDto);

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
        } catch (ServiceUnavailableException e) {
            // IMPORTANT: According to the "no retries" policy, temporary service unavailability
            // results in immediate and permanent payment failure. Business stakeholders should
            // be aware that temporary external service outages will result in permanent payment failures.
            log.error("Payment request failed due to service unavailable - PaymentId: {}, CustomerId: {}, ErrorMessage: {}. " +
                    "This is a permanent failure due to no-retry policy.",
                    paymentDto.getPaymentId(), paymentDto.getCustomerId(), e.getMessage());
            paymentResponseProducer.sendErrorResponse(paymentDto, e.getMessage());
        } catch (Exception e) {
            log.error("Payment request failed due to unexpected error - PaymentId: {}, CustomerId: {}, ErrorType: {}, ErrorMessage: {}",
                    paymentDto.getPaymentId(), paymentDto.getCustomerId(), e.getClass().getSimpleName(), e.getMessage());
            paymentResponseProducer.sendErrorResponse(paymentDto, "Payment processing failed due to unexpected error: " + e.getMessage());
        }
    }

}
