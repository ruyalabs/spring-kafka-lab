package ch.ruyalabs.springkafkalab.listener;

import ch.ruyalabs.springkafkalab.consumer.NonTransactionalPaymentResponseProducer;
import ch.ruyalabs.springkafkalab.consumer.PaymentExecutionStatusConsumer;
import ch.ruyalabs.springkafkalab.event.PaymentResponseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Event listener that handles payment response events after transaction commit.
 * This replaces the @Async approach with a more robust transactional event-driven approach.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentResponseEventListener {

    private final NonTransactionalPaymentResponseProducer nonTransactionalPaymentResponseProducer;
    private final PaymentExecutionStatusConsumer paymentExecutionStatusConsumer;

    /**
     * Handles payment response events after the transaction commits.
     * This method is automatically invoked by Spring only after the transaction 
     * from the consume method has successfully committed.
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handlePaymentResponseEvent(PaymentResponseEvent event) {
        String paymentId = event.getPaymentId();
        log.debug("Handling payment response event after transaction commit - PaymentId: {}", paymentId);

        PaymentExecutionStatusConsumer.PaymentExecutionResult result = paymentExecutionStatusConsumer.getPendingResponse(paymentId);
        if (result == null) {
            log.warn("No pending response found for PaymentId: {} - may have been already sent", paymentId);
            return;
        }

        // Atomic state transition: PENDING -> SENDING
        if (!result.compareAndSetState(PaymentExecutionStatusConsumer.ResponseState.PENDING, 
                                     PaymentExecutionStatusConsumer.ResponseState.SENDING)) {
            PaymentExecutionStatusConsumer.ResponseState currentState = result.getState();
            if (currentState == PaymentExecutionStatusConsumer.ResponseState.SENT) {
                log.debug("Response already sent for PaymentId: {} - skipping duplicate send attempt", paymentId);
                return;
            } else if (currentState == PaymentExecutionStatusConsumer.ResponseState.SENDING) {
                log.warn("Response already being sent for PaymentId: {} - skipping duplicate send attempt", paymentId);
                return;
            }
            log.warn("Unexpected state transition for PaymentId: {} - current state: {}", paymentId, currentState);
            return;
        }

        try {
            // Send the response
            if (result.isSuccess()) {
                nonTransactionalPaymentResponseProducer.sendSuccessResponse(result.getOriginalPayment());
            } else {
                nonTransactionalPaymentResponseProducer.sendErrorResponse(result.getOriginalPayment(), result.getErrorMessage());
            }

            // Atomic state transition: SENDING -> SENT and cleanup
            if (result.compareAndSetState(PaymentExecutionStatusConsumer.ResponseState.SENDING, 
                                        PaymentExecutionStatusConsumer.ResponseState.SENT)) {
                // Only perform cleanup if state transition was successful
                paymentExecutionStatusConsumer.markPaymentCompleted(paymentId);

                log.info("Payment response sent successfully with atomic state transition - PaymentId: {}, Status: {}",
                        paymentId, result.isSuccess() ? "success" : "error");
            } else {
                log.error("CRITICAL: Failed to transition state from SENDING to SENT for PaymentId: {} - " +
                        "response was sent but state is inconsistent. Current state: {}", 
                        paymentId, result.getState());
            }

        } catch (Exception e) {
            // Reset state back to PENDING for retry
            result.compareAndSetState(PaymentExecutionStatusConsumer.ResponseState.SENDING, 
                                    PaymentExecutionStatusConsumer.ResponseState.PENDING);

            log.error("Failed to send payment response - PaymentId: {}, ErrorMessage: {}, ErrorType: {}. " +
                            "State reset to PENDING for retry by scheduled task.",
                    paymentId, e.getMessage(), e.getClass().getSimpleName(), e);
        }
    }
}
