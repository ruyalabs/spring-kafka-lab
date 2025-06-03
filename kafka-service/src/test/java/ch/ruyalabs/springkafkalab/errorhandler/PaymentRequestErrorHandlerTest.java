package ch.ruyalabs.springkafkalab.errorhandler;

import ch.qos.logback.classic.Level;
import ch.ruyalabs.springkafkalab.util.LogCaptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class PaymentRequestErrorHandlerTest {

    private PaymentRequestErrorHandler errorHandler;
    private LogCaptor logCaptor;

    @BeforeEach
    void setUp() {
        // Create error handler with default values
        errorHandler = new PaymentRequestErrorHandler(
                Duration.ofSeconds(1),
                2.0,
                Duration.ofSeconds(60)
        );
        logCaptor = new LogCaptor(PaymentRequestErrorHandler.class.getName());
    }

    @AfterEach
    void tearDown() {
        logCaptor.stop();
    }

    @Test
    void constructor_shouldCreateValidErrorHandler() {
        // Simply verify that the constructor doesn't throw an exception
        assertNotNull(errorHandler);
    }

    @Test
    void constructor_shouldAcceptCustomParameters() {
        // Create error handler with custom values
        PaymentRequestErrorHandler customErrorHandler = new PaymentRequestErrorHandler(
                Duration.ofMillis(500),
                3.0,
                Duration.ofSeconds(30)
        );

        // Simply verify that the constructor doesn't throw an exception
        assertNotNull(customErrorHandler);
    }
}
