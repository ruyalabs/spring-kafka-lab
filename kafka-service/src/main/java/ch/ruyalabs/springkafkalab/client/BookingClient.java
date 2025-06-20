package ch.ruyalabs.springkafkalab.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

/**
 * Client for the Booking Service
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BookingClient {

    private final RestTemplate restTemplate;

    @Value("${services.booking.url:http://localhost:8082}")
    private String bookingServiceUrl;

    /**
     * Create a booking
     *
     * @param request the booking request
     * @return the booking response
     * @throws InvalidRequestException if the request is invalid
     * @throws ServiceException if there is an error communicating with the service
     */
    public BookingResponse createBooking(BookingRequest request) {
        log.info("Creating booking: {}", request);
        String url = bookingServiceUrl + "/api/bookings";

        try {
            ResponseEntity<BookingResponse> response = restTemplate.postForEntity(url, request, BookingResponse.class);
            BookingResponse bookingResponse = response.getBody();
            log.info("Created booking: {}", bookingResponse);
            return bookingResponse;
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.BAD_REQUEST) {
                log.warn("Invalid booking request: {}", request);
                throw new InvalidRequestException("Invalid booking request: " + e.getMessage());
            }
            log.error("Error creating booking: {} - {}", e.getStatusCode(), e.getMessage());
            throw new ServiceException("Error creating booking: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error creating booking: {}", e.getMessage(), e);
            throw new ServiceException("Failed to create booking", e);
        }
    }

    /**
     * Get booking details
     *
     * @param bookingId the booking ID
     * @return the booking details
     * @throws BookingNotFoundException if the booking does not exist
     * @throws ServiceException if there is an error communicating with the service
     */
    public BookingDetails getBookingDetails(String bookingId) {
        log.info("Getting booking details: {}", bookingId);
        String url = bookingServiceUrl + "/api/bookings/" + bookingId;

        try {
            ResponseEntity<BookingDetails> response = restTemplate.getForEntity(url, BookingDetails.class);
            BookingDetails details = response.getBody();
            log.info("Retrieved booking details: {}", details);
            return details;
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                log.warn("Booking not found: {}", bookingId);
                throw new BookingNotFoundException("Booking not found: " + bookingId);
            }
            log.error("Error retrieving booking details: {} - {}", e.getStatusCode(), e.getMessage());
            throw new ServiceException("Error retrieving booking details: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error retrieving booking details: {}", e.getMessage(), e);
            throw new ServiceException("Failed to retrieve booking details", e);
        }
    }

    /**
     * Wait for booking to be confirmed
     *
     * @param bookingId the booking ID
     * @param maxAttempts maximum number of attempts to check status
     * @param delayMs delay between attempts in milliseconds
     * @return the final booking details
     * @throws BookingNotFoundException if the booking does not exist
     * @throws BookingFailedException if the booking failed
     * @throws BookingTimeoutException if the booking did not complete within the specified attempts
     * @throws ServiceException if there is an error communicating with the service
     */
    public BookingDetails waitForBookingConfirmation(String bookingId, int maxAttempts, long delayMs) {
        log.info("Waiting for booking confirmation: {}", bookingId);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                BookingDetails details = getBookingDetails(bookingId);

                if ("CONFIRMED".equals(details.status())) {
                    log.info("Booking confirmed: {}", bookingId);
                    return details;
                } else if ("FAILED".equals(details.status())) {
                    log.warn("Booking failed: {}", bookingId);
                    throw new BookingFailedException("Booking failed: " + bookingId);
                }

                log.info("Booking status is {}, waiting... (attempt {}/{})", 
                        details.status(), attempt, maxAttempts);

                // Sleep before next attempt
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ServiceException("Interrupted while waiting for booking confirmation", ie);
                }
            } catch (BookingNotFoundException | ServiceException e) {
                // These exceptions should be propagated immediately
                throw e;
            }
        }

        log.warn("Booking confirmation timed out: {}", bookingId);
        throw new BookingTimeoutException("Booking confirmation timed out after " + maxAttempts + " attempts");
    }

    /**
     * DTO for booking request
     */
    public record BookingRequest(String userId, String item, int quantity) {}

    /**
     * DTO for booking response
     */
    public record BookingResponse(String bookingId, String item, String status) {}

    /**
     * DTO for booking details
     */
    public record BookingDetails(
        String bookingId,
        String userId,
        String item,
        int quantity,
        String status,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
    ) {}

    /**
     * Exception thrown when a booking is not found
     */
    public static class BookingNotFoundException extends RuntimeException {
        public BookingNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when a booking fails
     */
    public static class BookingFailedException extends RuntimeException {
        public BookingFailedException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when a booking confirmation times out
     */
    public static class BookingTimeoutException extends RuntimeException {
        public BookingTimeoutException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when a request is invalid
     */
    public static class InvalidRequestException extends RuntimeException {
        public InvalidRequestException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when there is an error communicating with the service
     */
    public static class ServiceException extends RuntimeException {
        public ServiceException(String message) {
            super(message);
        }

        public ServiceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
