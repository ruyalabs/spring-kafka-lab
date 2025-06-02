package ch.ruyalabs.springkafkalab.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

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
     */
    public BookingResponse createBooking(BookingRequest request) {
        log.info("Creating booking: {}", request);
        String url = bookingServiceUrl + "/api/bookings";
        
        try {
            BookingResponse response = restTemplate.postForObject(url, request, BookingResponse.class);
            log.info("Created booking: {}", response);
            return response;
        } catch (Exception e) {
            log.error("Error creating booking: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create booking", e);
        }
    }

    /**
     * DTO for booking request
     */
    public record BookingRequest(String userId, String item, int quantity) {}

    /**
     * DTO for booking response
     */
    public record BookingResponse(String bookingId, String item, String status) {}
}