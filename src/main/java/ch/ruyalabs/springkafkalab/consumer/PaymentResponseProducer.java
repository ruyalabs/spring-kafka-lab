package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.dto.PaymentResponseDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class PaymentResponseProducer {

    private final KafkaTemplate<String, PaymentResponseDto> kafkaTemplate;
    private final KafkaTemplate<String, PaymentResponseDto> nonTransactionalKafkaTemplate;

    public PaymentResponseProducer(
            @Qualifier("paymentResponseKafkaTemplate") KafkaTemplate<String, PaymentResponseDto> kafkaTemplate,
            @Qualifier("nonTransactionalPaymentResponseKafkaTemplate") KafkaTemplate<String, PaymentResponseDto> nonTransactionalKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.nonTransactionalKafkaTemplate = nonTransactionalKafkaTemplate;
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

    public void sendErrorResponseNonTransactional(PaymentDto originalRequest, String errorMessage) throws Exception {
        PaymentResponseDto response = createErrorResponse(originalRequest, errorMessage);
        sendResponseNonTransactional(response, originalRequest.getPaymentId());
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
        // Set MDC context for structured logging
        MDC.put("paymentId", paymentId);
        MDC.put("customerId", response.getCustomerId());
        MDC.put("status", response.getStatus().toString());
        MDC.put("topic", paymentResponseTopic);
        MDC.put("operation", "payment_response_sending");

        try {
            log.info("Sending payment response to Kafka topic",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "payment_response_sending"),
                    net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentId),
                    net.logstash.logback.argument.StructuredArguments.kv("status", response.getStatus()),
                    net.logstash.logback.argument.StructuredArguments.kv("topic", paymentResponseTopic),
                    net.logstash.logback.argument.StructuredArguments.kv("customerId", response.getCustomerId()));

            // Use synchronous send for transactional context
            SendResult<String, PaymentResponseDto> result =
                    kafkaTemplate.send(paymentResponseTopic, paymentId, response).get();

            log.info("Payment response sent successfully to Kafka topic",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "payment_response_sent"),
                    net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentId),
                    net.logstash.logback.argument.StructuredArguments.kv("topic", paymentResponseTopic),
                    net.logstash.logback.argument.StructuredArguments.kv("offset", result.getRecordMetadata().offset()),
                    net.logstash.logback.argument.StructuredArguments.kv("partition", result.getRecordMetadata().partition()));
        } catch (Exception e) {
            log.error("Failed to send payment response to Kafka topic",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "payment_response_send_failed"),
                    net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentId),
                    net.logstash.logback.argument.StructuredArguments.kv("topic", paymentResponseTopic),
                    net.logstash.logback.argument.StructuredArguments.kv("errorMessage", e.getMessage()),
                    net.logstash.logback.argument.StructuredArguments.kv("errorType", e.getClass().getSimpleName()),
                    e);
            throw e; // Propagate the original exception instead of wrapping it
        } finally {
            // Clear MDC context
            MDC.clear();
        }
    }

    private void sendResponseNonTransactional(PaymentResponseDto response, String paymentId) throws Exception {
        // Set MDC context for structured logging
        MDC.put("paymentId", paymentId);
        MDC.put("customerId", response.getCustomerId());
        MDC.put("status", response.getStatus().toString());
        MDC.put("topic", paymentResponseTopic);
        MDC.put("operation", "payment_response_sending_non_transactional");

        try {
            log.info("Sending payment response to Kafka topic using non-transactional template",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "payment_response_sending_non_transactional"),
                    net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentId),
                    net.logstash.logback.argument.StructuredArguments.kv("status", response.getStatus()),
                    net.logstash.logback.argument.StructuredArguments.kv("topic", paymentResponseTopic),
                    net.logstash.logback.argument.StructuredArguments.kv("customerId", response.getCustomerId()));

            // Use non-transactional template for error recovery scenarios
            SendResult<String, PaymentResponseDto> result =
                    nonTransactionalKafkaTemplate.send(paymentResponseTopic, paymentId, response).get();

            log.info("Payment response sent successfully to Kafka topic using non-transactional template",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "payment_response_sent_non_transactional"),
                    net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentId),
                    net.logstash.logback.argument.StructuredArguments.kv("topic", paymentResponseTopic),
                    net.logstash.logback.argument.StructuredArguments.kv("offset", result.getRecordMetadata().offset()),
                    net.logstash.logback.argument.StructuredArguments.kv("partition", result.getRecordMetadata().partition()));
        } catch (Exception e) {
            log.error("Failed to send payment response to Kafka topic using non-transactional template",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "payment_response_send_failed_non_transactional"),
                    net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentId),
                    net.logstash.logback.argument.StructuredArguments.kv("topic", paymentResponseTopic),
                    net.logstash.logback.argument.StructuredArguments.kv("errorMessage", e.getMessage()),
                    net.logstash.logback.argument.StructuredArguments.kv("errorType", e.getClass().getSimpleName()),
                    e);
            throw e; // Propagate the original exception instead of wrapping it
        } finally {
            // Clear MDC context
            MDC.clear();
        }
    }
}
