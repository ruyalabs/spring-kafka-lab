package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.dto.PaymentExecutionStatusDto;
import ch.ruyalabs.springkafkalab.event.PaymentResponseEvent;
import jakarta.validation.Valid;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentExecutionStatusConsumer {

    private final NonTransactionalPaymentResponseProducer nonTransactionalPaymentResponseProducer;
    private final ApplicationEventPublisher eventPublisher;

    private static final ConcurrentHashMap<String, PaymentDto> pendingPayments = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Boolean> completedPayments = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, PaymentExecutionResult> pendingResponses = new ConcurrentHashMap<>();

    public enum ResponseState {
        PENDING,
        SENDING,
        SENT
    }

    public static class PaymentExecutionResult {
        @Getter
        private final PaymentDto originalPayment;
        @Getter
        private final boolean isSuccess;
        @Getter
        private final String errorMessage;
        private final AtomicReference<ResponseState> state;
        @Getter
        private final long createdTimestamp;

        public PaymentExecutionResult(PaymentDto originalPayment, boolean isSuccess, String errorMessage) {
            this.originalPayment = originalPayment;
            this.isSuccess = isSuccess;
            this.errorMessage = errorMessage;
            this.state = new AtomicReference<>(ResponseState.PENDING);
            this.createdTimestamp = System.currentTimeMillis();
        }

        public ResponseState getState() {
            return state.get();
        }

        public boolean compareAndSetState(ResponseState expected, ResponseState update) {
            return state.compareAndSet(expected, update);
        }
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-execution-status}",
            containerFactory = "paymentExecutionStatusKafkaListenerContainerFactory"
    )
    @Transactional(transactionManager = "kafkaTransactionManager", rollbackFor = Exception.class)
    public void consume(@Payload @Valid PaymentExecutionStatusDto statusDto) throws Exception {

        log.info("Payment execution status consumed from Kafka topic: payment-execution-status - PaymentId: {}, Status: {}, Operation: payment_execution_status_processing",
                statusDto.getPaymentId(), statusDto.getStatus());

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

        boolean isSuccess = PaymentExecutionStatusDto.StatusEnum.OK.equals(statusDto.getStatus());
        String errorMessage = isSuccess ? null : "Payment execution failed";

        log.info("Payment execution {} - Status: {}, PaymentId: {}, CustomerId: {}",
                isSuccess ? "successful" : "failed",
                isSuccess ? "success" : "error",
                statusDto.getPaymentId(), originalPayment.getCustomerId());

        PaymentExecutionResult result = new PaymentExecutionResult(originalPayment, isSuccess, errorMessage);
        pendingResponses.put(statusDto.getPaymentId(), result);

        pendingPayments.remove(statusDto.getPaymentId());

        log.debug("Payment consumption completed successfully - PaymentId: {}, removed from pending, queued for response sending",
                statusDto.getPaymentId());

        eventPublisher.publishEvent(new PaymentResponseEvent(statusDto.getPaymentId()));
    }

    public void sendPendingResponseAsync(String paymentId) {
        PaymentExecutionResult result = pendingResponses.get(paymentId);
        if (result == null) {
            log.warn("No pending response found for PaymentId: {} - may have been already sent", paymentId);
            return;
        }
        if (!result.compareAndSetState(ResponseState.PENDING, ResponseState.SENDING)) {
            ResponseState currentState = result.getState();
            if (currentState == ResponseState.SENT) {
                log.debug("Response already sent for PaymentId: {} - skipping duplicate send attempt", paymentId);
                return;
            } else if (currentState == ResponseState.SENDING) {
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

            if (result.compareAndSetState(ResponseState.SENDING, ResponseState.SENT)) {
                completedPayments.put(paymentId, true);
                pendingResponses.remove(paymentId);

                log.info("Payment response sent successfully with atomic state transition - PaymentId: {}, Status: {}",
                        paymentId, result.isSuccess() ? "success" : "error");
            } else {
                log.error("CRITICAL: Failed to transition state from SENDING to SENT for PaymentId: {} - " +
                                "response was sent but state is inconsistent. Current state: {}",
                        paymentId, result.getState());
            }

        } catch (Exception e) {
            result.compareAndSetState(ResponseState.SENDING, ResponseState.PENDING);

            log.error("Failed to send payment response - PaymentId: {}, ErrorMessage: {}, ErrorType: {}. " +
                            "State reset to PENDING for retry by scheduled task.",
                    paymentId, e.getMessage(), e.getClass().getSimpleName(), e);
        }
    }


    @EventListener(ApplicationReadyEvent.class)
    public void processPendingResponsesOnStartup() {
        if (pendingResponses.isEmpty()) {
            log.info("No pending responses found on startup");
            return;
        }

        log.warn("RECOVERY: Found {} pending responses on startup - attempting to send them now. " +
                        "This indicates the application previously crashed after consuming messages but before sending responses.",
                pendingResponses.size());

        for (String paymentId : pendingResponses.keySet()) {
            PaymentExecutionResult result = pendingResponses.get(paymentId);
            if (result != null) {
                log.info("RECOVERY: Processing pending response on startup - PaymentId: {}, State: {}",
                        paymentId, result.getState());
                try {
                    sendPendingResponseAsync(paymentId);
                } catch (Exception e) {
                    log.error("RECOVERY: Failed to process pending response on startup - PaymentId: {}, ErrorMessage: {}, ErrorType: {}",
                            paymentId, e.getMessage(), e.getClass().getSimpleName(), e);
                }
            }
        }
    }

    @Scheduled(fixedDelay = 30000)
    public void retryPendingResponses() {
        if (pendingResponses.isEmpty()) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        int retriedCount = 0;
        int stuckCount = 0;

        for (String paymentId : pendingResponses.keySet()) {
            PaymentExecutionResult result = pendingResponses.get(paymentId);
            if (result == null) {
                continue;
            }

            ResponseState state = result.getState();
            long ageInSeconds = (currentTime - result.getCreatedTimestamp()) / 1000;

            if (state == ResponseState.PENDING) {
                if (ageInSeconds > 60) {
                    log.warn("RETRY: Attempting to send pending response - PaymentId: {}, Age: {}s",
                            paymentId, ageInSeconds);
                    try {
                        sendPendingResponseAsync(paymentId);
                        retriedCount++;
                    } catch (Exception e) {
                        log.error("RETRY: Failed to send pending response - PaymentId: {}, ErrorMessage: {}, ErrorType: {}",
                                paymentId, e.getMessage(), e.getClass().getSimpleName(), e);
                    }
                }
            } else if (state == ResponseState.SENDING && ageInSeconds > 300) {
                if (result.compareAndSetState(ResponseState.SENDING, ResponseState.PENDING)) {
                    log.error("RECOVERY: Response stuck in SENDING state - reset to PENDING for retry - PaymentId: {}, Age: {}s",
                            paymentId, ageInSeconds);
                }
            }

            if (ageInSeconds > 600) {
                stuckCount++;
                if (ageInSeconds % 300 == 0) {
                    log.error("ALERT: Response stuck in pending state for extended period - PaymentId: {}, State: {}, Age: {}s. " +
                                    "This may indicate a persistent issue with response sending.",
                            paymentId, state, ageInSeconds);
                }
            }
        }

        if (retriedCount > 0 || stuckCount > 0) {
            log.info("RETRY_SUMMARY: Retried {} responses, {} responses stuck for >10min, {} total pending",
                    retriedCount, stuckCount, pendingResponses.size());
        }
    }


    public static void addPendingPayment(String paymentId, PaymentDto paymentDto) {
        pendingPayments.put(paymentId, paymentDto);
        log.debug("Added pending payment - PaymentId: {}, CustomerId: {}", paymentId, paymentDto.getCustomerId());
    }


    public static int getPendingPaymentsCount() {
        return pendingPayments.size();
    }

    public static int getCompletedPaymentsCount() {
        return completedPayments.size();
    }

    public static void clearAllPayments() {
        pendingPayments.clear();
        completedPayments.clear();
    }

    public PaymentExecutionResult getPendingResponse(String paymentId) {
        return pendingResponses.get(paymentId);
    }

    public void markPaymentCompleted(String paymentId) {
        completedPayments.put(paymentId, true);
        pendingResponses.remove(paymentId);
    }
}
