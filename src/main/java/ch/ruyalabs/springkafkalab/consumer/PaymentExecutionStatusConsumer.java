package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.dto.PaymentExecutionStatusDto;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentExecutionStatusConsumer {

    private final PaymentResponseProducer paymentResponseProducer;
    private final NonTransactionalPaymentResponseProducer nonTransactionalPaymentResponseProducer;

    private static final ConcurrentHashMap<String, PaymentDto> pendingPayments = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Boolean> completedPayments = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, PaymentExecutionResult> pendingResponses = new ConcurrentHashMap<>();

    private static class PaymentExecutionResult {
        private final PaymentDto originalPayment;
        private final boolean isSuccess;
        private final String errorMessage;

        public PaymentExecutionResult(PaymentDto originalPayment, boolean isSuccess, String errorMessage) {
            this.originalPayment = originalPayment;
            this.isSuccess = isSuccess;
            this.errorMessage = errorMessage;
        }

        public PaymentDto getOriginalPayment() { return originalPayment; }
        public boolean isSuccess() { return isSuccess; }
        public String getErrorMessage() { return errorMessage; }
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-execution-status}",
            containerFactory = "paymentExecutionStatusKafkaListenerContainerFactory"
    )
    @Transactional(transactionManager = "kafkaTransactionManager", rollbackFor = Exception.class)
    public void consume(@Payload @Valid PaymentExecutionStatusDto statusDto) throws Exception {

        log.info("Payment execution status consumed from Kafka topic: payment-execution-status - PaymentId: {}, Status: {}, Operation: payment_execution_status_processing",
                statusDto.getPaymentId(), statusDto.getStatus());

        // Check if this payment has already been fully processed (both consumed and response sent)
        if (completedPayments.containsKey(statusDto.getPaymentId())) {
            log.info("Payment status already processed - acknowledging duplicate message without reprocessing - PaymentId: {}, Status: {}",
                    statusDto.getPaymentId(), statusDto.getStatus());
            return;
        }

        PaymentDto originalPayment = pendingPayments.get(statusDto.getPaymentId());

        if (originalPayment == null) {
            log.warn("No pending payment found for PaymentId: {} - Status message may be duplicate or orphaned",
                    statusDto.getPaymentId());
            return;
        }

        // Phase 1: Transactional consumption and state update
        // Determine the result and prepare for response sending
        boolean isSuccess = PaymentExecutionStatusDto.StatusEnum.OK.equals(statusDto.getStatus());
        String errorMessage = isSuccess ? null : "Payment execution failed";

        log.info("Payment execution {} - Status: {}, PaymentId: {}, CustomerId: {}",
                isSuccess ? "successful" : "failed", 
                isSuccess ? "success" : "error", 
                statusDto.getPaymentId(), originalPayment.getCustomerId());

        // Store the result for response sending after transaction commit
        PaymentExecutionResult result = new PaymentExecutionResult(originalPayment, isSuccess, errorMessage);
        pendingResponses.put(statusDto.getPaymentId(), result);

        // Remove from pending payments as consumption is complete
        pendingPayments.remove(statusDto.getPaymentId());

        log.debug("Payment consumption completed successfully - PaymentId: {}, removed from pending, queued for response sending",
                statusDto.getPaymentId());

        // Transaction will commit here, then sendPendingResponse will be called
        // Call async method to send response after transaction commit
        sendPendingResponseAsync(statusDto.getPaymentId());
    }

    /**
     * Send pending response asynchronously after transaction commit
     * 
     * IMPORTANT: This method introduces a risk of lost responses if the application
     * crashes after the Kafka consumer transaction commits but before the response
     * is successfully sent. This is a known limitation given the constraints:
     * - No Dead Letter Queue (DLQ)
     * - No retries allowed
     * - Exactly one response requirement
     * 
     * The processPendingResponsesOnStartup() method attempts to mitigate this risk
     * by resending pending responses on application restart, but it cannot guarantee
     * 100% reliability in all crash scenarios.
     */
    @Async
    public void sendPendingResponseAsync(String paymentId) {
        PaymentExecutionResult result = pendingResponses.get(paymentId);
        if (result == null) {
            log.warn("No pending response found for PaymentId: {} - may have been already sent", paymentId);
            return;
        }

        try {
            if (result.isSuccess()) {
                nonTransactionalPaymentResponseProducer.sendSuccessResponse(result.getOriginalPayment());
            } else {
                nonTransactionalPaymentResponseProducer.sendErrorResponse(result.getOriginalPayment(), result.getErrorMessage());
            }

            // Mark as completed and remove from pending responses
            completedPayments.put(paymentId, true);
            pendingResponses.remove(paymentId);

            log.info("Payment response sent successfully after transaction commit - PaymentId: {}, Status: {}",
                    paymentId, result.isSuccess() ? "success" : "error");

        } catch (Exception e) {
            log.error("CRITICAL: Failed to send payment response after transaction commit - PaymentId: {}, ErrorMessage: {}, ErrorType: {}. " +
                            "This violates the exactly-one-response guarantee. Response will remain in pending state for retry on next startup, " +
                            "but there is a risk of permanent response loss if the failure persists.",
                    paymentId, e.getMessage(), e.getClass().getSimpleName(), e);
            // Note: We don't remove from pendingResponses so it can be retried on startup
            // However, if the failure is persistent (e.g., Kafka broker down permanently),
            // the response may never be sent, violating the "no request may go unanswered" rule
        }
    }

    /**
     * Process any pending responses on application startup
     * This handles cases where the application restarted after consuming messages but before sending responses
     * 
     * IMPORTANT: This method provides a best-effort attempt to recover from application crashes
     * that occur after Kafka transaction commit but before response sending. However, it cannot
     * guarantee 100% reliability:
     * - If the application crashes during this startup process, responses may still be lost
     * - If response sending fails persistently (e.g., due to Kafka broker issues), responses
     *   will remain in pending state indefinitely
     * - The pendingResponses map is in-memory only, so it's lost on application restart
     * 
     * This is a known limitation given the constraints of no DLQ and no retries.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void processPendingResponsesOnStartup() {
        if (pendingResponses.isEmpty()) {
            log.info("No pending responses found on startup");
            return;
        }

        log.warn("RECOVERY: Found {} pending responses on startup - attempting to send them now. " +
                "This indicates the application previously crashed after consuming messages but before sending responses.",
                pendingResponses.size());

        // Process all pending responses
        for (String paymentId : pendingResponses.keySet()) {
            log.info("RECOVERY: Processing pending response on startup - PaymentId: {}", paymentId);
            try {
                sendPendingResponseAsync(paymentId);
            } catch (Exception e) {
                log.error("RECOVERY: Failed to process pending response on startup - PaymentId: {}, ErrorMessage: {}, ErrorType: {}",
                        paymentId, e.getMessage(), e.getClass().getSimpleName(), e);
                // Continue processing other pending responses even if one fails
            }
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
