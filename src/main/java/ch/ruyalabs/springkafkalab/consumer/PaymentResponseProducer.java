package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.dto.PaymentResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class PaymentResponseProducer {

    private final KafkaTemplate<String, PaymentResponseDto> kafkaTemplate;

    public PaymentResponseProducer(
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, PaymentResponseDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Value("${app.kafka.topics.payment-response}")
    private String paymentResponseTopic;

    public void sendSuccessResponse(PaymentDto originalRequest) throws Exception {
        PaymentResponseDto response = createSuccessResponse(originalRequest);
        sendResponse(response, originalRequest.getPaymentId());
    }

    public void sendErrorResponse(PaymentDto originalRequest, String errorMessage) throws Exception {
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

    private void sendResponse(PaymentResponseDto response, String paymentId) throws Exception {
        log.info("Sending payment response to Kafka topic - Operation: payment_response_sending, PaymentId: {}, CustomerId: {}, Status: {}, Topic: {}",
                paymentId, response.getCustomerId(), response.getStatus(), paymentResponseTopic);

        try {
            SendResult<String, PaymentResponseDto> result =
                    kafkaTemplate.send(paymentResponseTopic, paymentId, response).get();

            log.info("Payment response sent successfully to Kafka topic - PaymentId: {}, Topic: {}, Offset: {}, Partition: {}",
                    paymentId, paymentResponseTopic, result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
        } catch (Exception e) {
            log.error("Failed to send payment response to Kafka topic - PaymentId: {}, Topic: {}, ErrorMessage: {}, ErrorType: {}",
                    paymentId, paymentResponseTopic, e.getMessage(), e.getClass().getSimpleName(), e);
            throw e;
        }
    }
}
