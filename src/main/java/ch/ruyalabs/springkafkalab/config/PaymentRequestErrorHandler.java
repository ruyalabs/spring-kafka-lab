package ch.ruyalabs.springkafkalab.config;

import ch.ruyalabs.springkafkalab.consumer.PaymentResponseProducer;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.io.PrintWriter;
import java.io.StringWriter;

@Slf4j
@Component
public class PaymentRequestErrorHandler extends DefaultErrorHandler {

    private final PaymentResponseProducer paymentResponseProducer;

    public PaymentRequestErrorHandler(PaymentResponseProducer paymentResponseProducer) {
        super(new PaymentRequestRecoverer(paymentResponseProducer), new FixedBackOff(1000L, 3L));
        this.paymentResponseProducer = paymentResponseProducer;
    }

    @Override
    public void handleRemaining(Exception thrownException,
                                java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<?, ?>> records,
                                Consumer<?, ?> consumer,
                                org.springframework.kafka.listener.MessageListenerContainer container) {

        log.error("PaymentRequestErrorHandler: Handling remaining records after retries exhausted. Exception: {}",
                thrownException.getMessage(), thrownException);

        for (org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record : records) {
            logRetryAttempt(record, thrownException, "FINAL_FAILURE");
        }

        super.handleRemaining(thrownException, records, consumer, container);
    }

    @Override
    public boolean handleOne(Exception thrownException,
                             org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                             Consumer<?, ?> consumer,
                             org.springframework.kafka.listener.MessageListenerContainer container) {

        logRetryAttempt(record, thrownException, "RETRY_ATTEMPT");

        return super.handleOne(thrownException, record, consumer, container);
    }

    private void logRetryAttempt(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                                 Exception exception, String attemptType) {
        String stackTrace = getStackTrace(exception);

        log.error("PaymentRequestErrorHandler: {} for record - Topic: {}, Partition: {}, Offset: {}, Key: {}",
                attemptType, record.topic(), record.partition(), record.offset(), record.key());
        log.error("Exception: {}", exception.getMessage());
        log.error("Stack trace: {}", stackTrace);
    }

    private String getStackTrace(Exception exception) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);
        return sw.toString();
    }

    @RequiredArgsConstructor
    private static class PaymentRequestRecoverer implements ConsumerRecordRecoverer {

        private final PaymentResponseProducer paymentResponseProducer;

        @Override
        public void accept(ConsumerRecord<?, ?> record, Exception exception) {
            log.error("PaymentRequestRecoverer: Processing failed record after all retries exhausted");
            log.error("Record details - Topic: {}, Partition: {}, Offset: {}, Key: {}",
                    record.topic(), record.partition(), record.offset(), record.key());
            log.error("Final failure exception: {}", exception.getMessage(), exception);

            try {
                // Try to extract PaymentDto from the record
                PaymentDto paymentDto = extractPaymentDto(record, exception);

                if (paymentDto != null) {
                    String errorMessage = buildErrorMessage(exception);
                    paymentResponseProducer.sendErrorResponse(paymentDto, errorMessage);
                    log.info("Error response sent for paymentId: {}", paymentDto.getPaymentId());
                } else {
                    log.error("Could not extract PaymentDto from failed record, unable to send error response");
                }
            } catch (Exception e) {
                log.error("Exception occurred while sending error response in recoverer: {}", e.getMessage(), e);
            }
        }

        private PaymentDto extractPaymentDto(ConsumerRecord<?, ?> record, Exception exception) {
            try {
                // If the record value is already a PaymentDto (successful deserialization)
                if (record.value() instanceof PaymentDto) {
                    return (PaymentDto) record.value();
                }

                // If it's a deserialization exception, we might not be able to recover the PaymentDto
                if (exception instanceof DeserializationException) {
                    log.warn("Deserialization exception occurred, cannot extract PaymentDto from record");
                    return null;
                }

                // For other exceptions, the value should be available
                if (record.value() instanceof PaymentDto) {
                    return (PaymentDto) record.value();
                }

                log.warn("Record value is not a PaymentDto: {}", record.value());
                return null;

            } catch (Exception e) {
                log.error("Exception while extracting PaymentDto: {}", e.getMessage(), e);
                return null;
            }
        }

        private String buildErrorMessage(Exception exception) {
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("Payment processing failed after all retry attempts. ");
            errorMsg.append("Error: ").append(exception.getMessage());

            if (exception.getCause() != null) {
                errorMsg.append(" Cause: ").append(exception.getCause().getMessage());
            }

            return errorMsg.toString();
        }
    }
}
