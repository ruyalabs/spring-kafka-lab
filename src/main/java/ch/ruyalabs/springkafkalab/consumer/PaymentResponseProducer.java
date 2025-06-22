package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.dto.PaymentResponseDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentResponseProducer {

    private final KafkaTemplate<String, PaymentResponseDto> kafkaTemplate;

    @Value("${app.kafka.topics.payment-response}")
    private String paymentResponseTopic;

    public void sendSuccessResponse(PaymentDto originalRequest) {
        PaymentResponseDto response = createSuccessResponse(originalRequest);
        sendResponse(response, originalRequest.getPaymentId());
    }

    public void sendErrorResponse(PaymentDto originalRequest, String errorMessage) {
        PaymentResponseDto response = createErrorResponse(originalRequest, errorMessage);
        sendResponse(response, originalRequest.getPaymentId());
    }

    private PaymentResponseDto createSuccessResponse(PaymentDto originalRequest) {
        return new PaymentResponseDto()
                .paymentId(originalRequest.getPaymentId())
                .amount(originalRequest.getAmount())
                .currency(originalRequest.getCurrency())
                .paymentMethod(PaymentResponseDto.PaymentMethodEnum.fromValue(originalRequest.getPaymentMethod().getValue()))
                .customerId(originalRequest.getCustomerId())
                .status(PaymentResponseDto.StatusEnum.SUCCESS);
    }

    private PaymentResponseDto createErrorResponse(PaymentDto originalRequest, String errorMessage) {
        return new PaymentResponseDto()
                .paymentId(originalRequest.getPaymentId())
                .amount(originalRequest.getAmount())
                .currency(originalRequest.getCurrency())
                .paymentMethod(PaymentResponseDto.PaymentMethodEnum.fromValue(originalRequest.getPaymentMethod().getValue()))
                .customerId(originalRequest.getCustomerId())
                .status(PaymentResponseDto.StatusEnum.ERROR)
                .errorInfo(errorMessage);
    }

    private void sendResponse(PaymentResponseDto response, String paymentId) {
        try {
            log.info("Sending payment response for paymentId: {}, status: {}", paymentId, response.getStatus());

            CompletableFuture<SendResult<String, PaymentResponseDto>> future = 
                kafkaTemplate.send(paymentResponseTopic, paymentId, response);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Successfully sent payment response for paymentId: {} to topic: {} at offset: {}", 
                            paymentId, paymentResponseTopic, result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to send payment response for paymentId: {} to topic: {}", 
                            paymentId, paymentResponseTopic, ex);
                }
            });
        } catch (Exception e) {
            log.error("Exception occurred while sending payment response for paymentId: {}", paymentId, e);
        }
    }
}
