package ch.ruyalabs.springkafkalab.producer;

import ch.ruyalabs.springkafkalab.dto.ErrorResponseDto;
import ch.ruyalabs.springkafkalab.dto.SuccessResponseDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Producer for sending payment responses to the payment-response topic.
 * Guarantees that a response is sent for every request, either success or error.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentResponseProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${payment.response.topic:payment-response}")
    private String responseTopicName;

    /**
     * Sends a success response to the payment-response topic.
     * 
     * @param requestId the original request ID
     * @param transactionId the generated transaction ID
     * @param message optional success message
     */
    public void sendSuccessResponse(String requestId, String transactionId, String message) {
        try {
            SuccessResponseDto response = new SuccessResponseDto(
                requestId,
                "SUCCESS",
                OffsetDateTime.now()
            );
            response.setTransactionId(transactionId);
            response.setMessage(message != null ? message : "Payment processed successfully");

            sendResponse(requestId, response, "SUCCESS");
            
        } catch (Exception e) {
            log.error("[DEBUG_LOG] Failed to send success response for request {}: {}", requestId, e.getMessage(), e);
            // If we can't send success response, try to send an error response instead
            sendTechnicalErrorResponse(requestId, "Failed to send success response: " + e.getMessage());
        }
    }

    /**
     * Sends an error response to the payment-response topic.
     * 
     * @param requestId the original request ID
     * @param errorCode the error code
     * @param errorMessage the error message
     * @param isFunctionalError true if it's a functional error, false for technical error
     */
    public void sendErrorResponse(String requestId, ErrorResponseDto.ErrorCodeEnum errorCode, 
                                String errorMessage, boolean isFunctionalError) {
        try {
            ErrorResponseDto response = new ErrorResponseDto(
                requestId,
                errorCode,
                errorMessage,
                OffsetDateTime.now()
            );
            response.setRetryable(false); // As per requirements, all exceptions are non-retryable

            String errorType = isFunctionalError ? "FUNCTIONAL" : "TECHNICAL";
            sendResponse(requestId, response, "ERROR_" + errorType);
            
        } catch (Exception e) {
            log.error("[DEBUG_LOG] Failed to send error response for request {}: {}", requestId, e.getMessage(), e);
            // Last resort: try to send a basic technical error response
            sendTechnicalErrorResponseLastResort(requestId, "Failed to send error response: " + e.getMessage());
        }
    }

    /**
     * Sends a technical error response when other response sending fails.
     * 
     * @param requestId the original request ID
     * @param errorMessage the error message
     */
    private void sendTechnicalErrorResponse(String requestId, String errorMessage) {
        try {
            ErrorResponseDto response = new ErrorResponseDto(
                requestId,
                ErrorResponseDto.ErrorCodeEnum.TECHNICAL_ERROR,
                errorMessage,
                OffsetDateTime.now()
            );
            response.setRetryable(false);

            sendResponse(requestId, response, "ERROR_TECHNICAL");
            
        } catch (Exception e) {
            log.error("[DEBUG_LOG] Failed to send technical error response for request {}: {}", requestId, e.getMessage(), e);
        }
    }

    /**
     * Last resort method to send a technical error response with minimal dependencies.
     * 
     * @param requestId the original request ID
     * @param errorMessage the error message
     */
    private void sendTechnicalErrorResponseLastResort(String requestId, String errorMessage) {
        try {
            // Create the most basic error response possible
            ErrorResponseDto response = new ErrorResponseDto();
            response.setId(requestId != null ? requestId : "unknown");
            response.setErrorCode(ErrorResponseDto.ErrorCodeEnum.TECHNICAL_ERROR);
            response.setErrorMessage("Critical error: " + errorMessage);
            response.setTimestamp(OffsetDateTime.now());
            response.setRetryable(false);

            // Use fire-and-forget to minimize chance of failure
            kafkaTemplate.send(responseTopicName, requestId, response);
            log.error("[DEBUG_LOG] Sent last resort error response for request {}", requestId);
            
        } catch (Exception e) {
            log.error("[DEBUG_LOG] CRITICAL: Failed to send last resort error response for request {}: {}", 
                     requestId, e.getMessage(), e);
            // At this point, we've exhausted all options
        }
    }

    /**
     * Internal method to send response with proper logging and error handling.
     * 
     * @param requestId the original request ID
     * @param response the response object
     * @param responseType the type of response for logging
     */
    private void sendResponse(String requestId, Object response, String responseType) {
        try {
            log.info("[DEBUG_LOG] Sending {} response for request {} to topic {}", 
                    responseType, requestId, responseTopicName);

            CompletableFuture<SendResult<String, Object>> future = 
                kafkaTemplate.send(responseTopicName, requestId, response);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("[DEBUG_LOG] Successfully sent {} response for request {} to partition {} at offset {}", 
                            responseType, requestId, 
                            result.getRecordMetadata().partition(), 
                            result.getRecordMetadata().offset());
                } else {
                    log.error("[DEBUG_LOG] Failed to send {} response for request {}: {}", 
                             responseType, requestId, ex.getMessage(), ex);
                    throw new RuntimeException("Failed to send response", ex);
                }
            });

            // Wait for the send to complete to ensure we know if it succeeded
            future.get();
            
        } catch (Exception e) {
            log.error("[DEBUG_LOG] Exception while sending {} response for request {}: {}", 
                     responseType, requestId, e.getMessage(), e);
            throw new RuntimeException("Failed to send response", e);
        }
    }
}