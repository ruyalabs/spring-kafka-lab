package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.dto.PaymentExecutionStatusDto;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentExecutionStatusConsumer {

    private final PaymentResponseProducer paymentResponseProducer;

    private static final ConcurrentHashMap<String, PaymentDto> pendingPayments = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Boolean> completedPayments = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = "${app.kafka.topics.payment-execution-status}",
            containerFactory = "paymentRequestKafkaListenerContainerFactory"
    )
    @Transactional(transactionManager = "kafkaTransactionManager", rollbackFor = Exception.class)
    public void consume(@Payload @Valid PaymentExecutionStatusDto statusDto,
                        @Header(value = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer deliveryAttempt) throws Exception {

        log.info("Payment execution status consumed from Kafka topic: payment-execution-status - PaymentId: {}, Status: {}, DeliveryAttempt: {}, Operation: payment_execution_status_processing",
                statusDto.getPaymentId(), statusDto.getStatus(), deliveryAttempt);

        if (deliveryAttempt != null && deliveryAttempt > 3) {
            log.warn("Poison pill detected for payment execution status - message has been retried {} times - PaymentId: {}",
                    deliveryAttempt, statusDto.getPaymentId());
            return;
        }

        // Check if this payment has already been processed (idempotency check)
        if (completedPayments.containsKey(statusDto.getPaymentId())) {
            log.info("Payment status already processed - acknowledging duplicate message without reprocessing - PaymentId: {}, Status: {}",
                    statusDto.getPaymentId(), statusDto.getStatus());
            return;
        }

        // Get the original payment but don't remove it yet (atomic operation requirement)
        PaymentDto originalPayment = pendingPayments.get(statusDto.getPaymentId());

        if (originalPayment == null) {
            log.warn("No pending payment found for PaymentId: {} - Status message may be duplicate or orphaned",
                    statusDto.getPaymentId());
            return;
        }

        try {
            // Send response first (within transaction)
            if (PaymentExecutionStatusDto.StatusEnum.OK.equals(statusDto.getStatus())) {
                log.info("Payment execution successful - Status: success, PaymentId: {}, CustomerId: {}",
                        statusDto.getPaymentId(), originalPayment.getCustomerId());
                paymentResponseProducer.sendSuccessResponse(originalPayment);
            } else {
                log.info("Payment execution failed - Status: error, PaymentId: {}, CustomerId: {}",
                        statusDto.getPaymentId(), originalPayment.getCustomerId());
                paymentResponseProducer.sendErrorResponse(originalPayment, "Payment execution failed");
            }

            // Only after successful response sending, update the state atomically
            pendingPayments.remove(statusDto.getPaymentId());
            completedPayments.put(statusDto.getPaymentId(), true);

            log.debug("Payment processing completed successfully - PaymentId: {}, removed from pending, marked as completed",
                    statusDto.getPaymentId());

        } catch (Exception e) {
            log.error("Failed to send payment response - keeping payment in pending state for retry - PaymentId: {}, ErrorMessage: {}, ErrorType: {}",
                    statusDto.getPaymentId(), e.getMessage(), e.getClass().getSimpleName());
            // Don't update state - payment remains in pendingPayments for potential retry
            throw e; // Re-throw to trigger transaction rollback
        }
    }

    /**
     * Store a pending payment request to be matched with execution status later
     */
    public static void addPendingPayment(String paymentId, PaymentDto paymentDto) {
        pendingPayments.put(paymentId, paymentDto);
        log.debug("Added pending payment - PaymentId: {}, CustomerId: {}", paymentId, paymentDto.getCustomerId());
    }

    /**
     * Get count of pending payments (for monitoring/debugging)
     */
    public static int getPendingPaymentsCount() {
        return pendingPayments.size();
    }

    /**
     * Get count of completed payments (for monitoring/debugging)
     */
    public static int getCompletedPaymentsCount() {
        return completedPayments.size();
    }

    /**
     * Clear all pending and completed payments (for testing purposes)
     */
    public static void clearAllPayments() {
        pendingPayments.clear();
        completedPayments.clear();
    }
}
