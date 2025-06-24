package ch.ruyalabs.springkafkalab.listener;

import ch.ruyalabs.springkafkalab.consumer.NonTransactionalPaymentResponseProducer;
import ch.ruyalabs.springkafkalab.consumer.PaymentExecutionStatusConsumer;
import ch.ruyalabs.springkafkalab.event.PaymentResponseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;


@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentResponseEventListener {

    private final NonTransactionalPaymentResponseProducer nonTransactionalPaymentResponseProducer;
    private final PaymentExecutionStatusConsumer paymentExecutionStatusConsumer;


    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handlePaymentResponseEvent(PaymentResponseEvent event) {
        String paymentId = event.getPaymentId();
        log.debug("Handling payment response event after transaction commit - PaymentId: {}", paymentId);

        PaymentExecutionStatusConsumer.PaymentExecutionResult result = paymentExecutionStatusConsumer.getPendingResponse(paymentId);
        if (result == null) {
            log.warn("No pending response found for PaymentId: {} - may have been already sent", paymentId);
            return;
        }

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
            if (result.isSuccess()) {
                nonTransactionalPaymentResponseProducer.sendSuccessResponse(result.getOriginalPayment());
            } else {
                nonTransactionalPaymentResponseProducer.sendErrorResponse(result.getOriginalPayment(), result.getErrorMessage());
            }

            if (result.compareAndSetState(PaymentExecutionStatusConsumer.ResponseState.SENDING,
                    PaymentExecutionStatusConsumer.ResponseState.SENT)) {
                paymentExecutionStatusConsumer.markPaymentCompleted(paymentId);

                log.info("Payment response sent successfully with atomic state transition - PaymentId: {}, Status: {}",
                        paymentId, result.isSuccess() ? "success" : "error");
            } else {
                log.error("CRITICAL: Failed to transition state from SENDING to SENT for PaymentId: {} - " +
                                "response was sent but state is inconsistent. Current state: {}",
                        paymentId, result.getState());
            }

        } catch (Exception e) {
            result.compareAndSetState(PaymentExecutionStatusConsumer.ResponseState.SENDING,
                    PaymentExecutionStatusConsumer.ResponseState.PENDING);

            log.error("Failed to send payment response - PaymentId: {}, ErrorMessage: {}, ErrorType: {}. " +
                            "State reset to PENDING for retry by scheduled task.",
                    paymentId, e.getMessage(), e.getClass().getSimpleName(), e);
        }
    }
}
