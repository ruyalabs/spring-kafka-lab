package ch.ruyalabs.bookingservice.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for booking operations
 */
@Slf4j
@RestController
@RequestMapping("/api/bookings")
public class BookingController {

    /**
     * Create a new booking
     *
     * @param request the booking request
     * @return ResponseEntity with the booking response
     */
    @PostMapping
    public ResponseEntity<BookingResponse> createBooking(@RequestBody BookingRequest request) {
        log.info("Creating booking: {}", request);

        try {
            // Dummy implementation - in a real application, this would create a booking in a database
            BookingResponse response = new BookingResponse(
                "booking-" + System.currentTimeMillis(), 
                request.item(), 
                "CONFIRMED"
            );
            log.info("Created booking: {}", response);
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        } catch (Exception e) {
            log.error("Error creating booking: {}", e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
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
