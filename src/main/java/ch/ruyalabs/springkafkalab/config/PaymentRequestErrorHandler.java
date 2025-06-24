package ch.ruyalabs.springkafkalab.config;

import ch.ruyalabs.springkafkalab.consumer.PaymentResponseProducer;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

@Slf4j
@Component
public class PaymentRequestErrorHandler extends DefaultErrorHandler {

    public PaymentRequestErrorHandler(PaymentResponseProducer paymentResponseProducer) {
        super(new PaymentRequestRecoverer(paymentResponseProducer));

        //I ADDED THIS
        this.setClassifications(Collections.emptyMap(), false);
    }


    private record PaymentRequestRecoverer(
            PaymentResponseProducer paymentResponseProducer) implements ConsumerRecordRecoverer {

        @Override
        public void accept(ConsumerRecord<?, ?> record, Exception exception) {
            log.error("Record recoverer processing failed record - Operation: record_recovery, Topic: {}, Partition: {}, Offset: {}, Key: {}, ExceptionType: {}, ExceptionMessage: {}",
                    record.topic(), record.partition(), record.offset(), record.key(), exception.getClass().getSimpleName(), exception.getMessage());

            try {
                PaymentDto paymentDto = extractPaymentDto(record, exception);

                if (paymentDto != null) {
                    String errorMessage = buildErrorMessage(exception);
                    paymentResponseProducer.sendErrorResponseNonTransactional(paymentDto, errorMessage);
                    log.info("Error response sent successfully for failed payment - PaymentId: {}, CustomerId: {}",
                            paymentDto.getPaymentId(), paymentDto.getCustomerId());
                } else {
                    if (exception instanceof DeserializationException) {
                        logRawMessage(record, exception);

                        String errorMessage = "Message deserialization failed: " + exception.getMessage();
                        String deterministicKey = generateDeterministicKey(record);
                        paymentResponseProducer.sendGenericDeserializationErrorResponse(errorMessage, deterministicKey);

                        log.info("Generic error response sent for deserialization failure - Reason: deserialization_failure, DeterministicKey: {}", deterministicKey);
                    } else {
                        log.error("Could not extract PaymentDto from failed record - Reason: unable_to_extract_payment_dto");
                    }
                }
            } catch (Exception e) {
                log.error("Exception occurred while sending error response in recoverer - ErrorType: {}, ErrorMessage: {}",
                        e.getClass().getSimpleName(), e.getMessage());
            }
        }

        private PaymentDto extractPaymentDto(ConsumerRecord<?, ?> record, Exception exception) {
            try {
                if (record.value() instanceof PaymentDto) {
                    return (PaymentDto) record.value();
                }

                if (exception instanceof DeserializationException) {
                    log.warn("Deserialization exception occurred, cannot extract PaymentDto from record - Reason: deserialization_exception, ExceptionType: {}",
                            exception.getClass().getSimpleName());
                    return null;
                }

                if (record.value() instanceof PaymentDto) {
                    return (PaymentDto) record.value();
                }

                log.warn("Record value is not a PaymentDto - Reason: invalid_record_value_type, RecordValueType: {}",
                        record.value() != null ? record.value().getClass().getSimpleName() : "null");
                return null;

            } catch (Exception e) {
                log.error("Exception while extracting PaymentDto - ErrorType: {}, ErrorMessage: {}",
                        e.getClass().getSimpleName(), e.getMessage());
                return null;
            }
        }

        private String buildErrorMessage(Exception exception) {
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("Payment processing failed. ");
            errorMsg.append("Error: ").append(exception.getMessage());

            if (exception.getCause() != null) {
                errorMsg.append(" Cause: ").append(exception.getCause().getMessage());
            }

            return errorMsg.toString();
        }

        private void logRawMessage(ConsumerRecord<?, ?> record, Exception exception) {
            try {
                byte[] rawValue = null;
                if (record.value() instanceof byte[]) {
                    rawValue = (byte[]) record.value();
                } else if (record.value() != null) {
                    rawValue = record.value().toString().getBytes();
                }

                String rawValueHex = rawValue != null ? bytesToHex(rawValue) : "null";
                String rawValueString = rawValue != null ? new String(rawValue) : "null";

                log.error("Raw message content for manual investigation - Topic: {}, Partition: {}, Offset: {}, Key: {}, RawValueHex: {}, RawValueString: {}, RawValueLength: {}, ExceptionType: {}, ExceptionMessage: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), rawValueHex, rawValueString,
                        rawValue != null ? rawValue.length : 0, exception.getClass().getSimpleName(), exception.getMessage());
            } catch (Exception e) {
                log.error("Failed to log raw message content - ErrorType: {}, ErrorMessage: {}",
                        e.getClass().getSimpleName(), e.getMessage());
            }
        }

        private String bytesToHex(byte[] bytes) {
            StringBuilder result = new StringBuilder();
            for (byte b : bytes) {
                result.append(String.format("%02x", b));
            }
            return result.toString();
        }

        private String generateDeterministicKey(ConsumerRecord<?, ?> record) {
            try {
                // Create a string that uniquely identifies this message
                StringBuilder keyBuilder = new StringBuilder();
                keyBuilder.append("topic:").append(record.topic());
                keyBuilder.append("|partition:").append(record.partition());
                keyBuilder.append("|offset:").append(record.offset());

                // Add raw message content if available
                if (record.value() != null) {
                    byte[] rawValue = null;
                    if (record.value() instanceof byte[]) {
                        rawValue = (byte[]) record.value();
                    } else {
                        rawValue = record.value().toString().getBytes();
                    }
                    keyBuilder.append("|payload:").append(bytesToHex(rawValue));
                }

                // Generate SHA-256 hash of the combined string
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] hash = digest.digest(keyBuilder.toString().getBytes());

                // Convert to hex and take first 16 characters for a shorter key
                String fullHash = bytesToHex(hash);
                return "DESER_ERR_" + fullHash.substring(0, 16);

            } catch (NoSuchAlgorithmException e) {
                log.error("Failed to generate deterministic key due to missing SHA-256 algorithm - ErrorType: {}, ErrorMessage: {}",
                        e.getClass().getSimpleName(), e.getMessage());
                // Fallback to a combination of topic, partition, and offset
                return "DESER_ERR_" + record.topic() + "_" + record.partition() + "_" + record.offset();
            } catch (Exception e) {
                log.error("Failed to generate deterministic key - ErrorType: {}, ErrorMessage: {}",
                        e.getClass().getSimpleName(), e.getMessage());
                // Ultimate fallback
                return "DESER_ERR_UNKNOWN";
            }
        }
    }
}
