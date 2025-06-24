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

        PaymentDto originalPayment = pendingPayments.remove(statusDto.getPaymentId());

        if (originalPayment == null) {
            log.warn("No pending payment found for PaymentId: {} - Status message may be duplicate or orphaned",
                    statusDto.getPaymentId());
            return;
        }

        if (PaymentExecutionStatusDto.StatusEnum.OK.equals(statusDto.getStatus())) {
            log.info("Payment execution successful - Status: success, PaymentId: {}, CustomerId: {}",
                    statusDto.getPaymentId(), originalPayment.getCustomerId());
            paymentResponseProducer.sendSuccessResponse(originalPayment);
        } else {
            log.info("Payment execution failed - Status: error, PaymentId: {}, CustomerId: {}",
                    statusDto.getPaymentId(), originalPayment.getCustomerId());
            paymentResponseProducer.sendErrorResponse(originalPayment, "Payment execution failed");
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
}