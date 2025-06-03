package ch.ruyalabs.bookingservice.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Controller for booking operations
 */
@Slf4j
@RestController
@RequestMapping("/api/bookings")
public class BookingController {

    // In-memory storage for bookings
    private final Map<String, Booking> bookings = new ConcurrentHashMap<>();

    // Executor service for simulating async processing
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

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
            // Validate request
            if (request.userId() == null || request.userId().isEmpty()) {
                log.warn("Invalid booking request - missing userId: {}", request);
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }

            if (request.item() == null || request.item().isEmpty()) {
                log.warn("Invalid booking request - missing item: {}", request);
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }

            if (request.quantity() <= 0) {
                log.warn("Invalid booking request - invalid quantity: {}", request.quantity());
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }

            // Generate booking ID
            String bookingId = "booking-" + System.currentTimeMillis();

            // Create booking with PENDING status
            Booking booking = new Booking(
                bookingId,
                request.userId(),
                request.item(),
                request.quantity(),
                "PENDING",
                LocalDateTime.now(),
                null
            );

            // Store booking
            bookings.put(bookingId, booking);

            // Simulate async processing - randomly confirm or fail the booking after a delay
            scheduler.schedule(() -> {
                // 80% chance of success
                boolean success = Math.random() < 0.8;
                String finalStatus = success ? "CONFIRMED" : "FAILED";

                Booking updatedBooking = new Booking(
                    booking.bookingId(),
                    booking.userId(),
                    booking.item(),
                    booking.quantity(),
                    finalStatus,
                    booking.createdAt(),
                    LocalDateTime.now()
                );

                bookings.put(bookingId, updatedBooking);
                log.info("Updated booking status: {} -> {}", bookingId, finalStatus);
            }, 2, TimeUnit.SECONDS);

            // Return response with PENDING status
            BookingResponse response = new BookingResponse(
                bookingId,
                request.item(),
                "PENDING"
            );

            log.info("Created booking: {}", response);
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        } catch (Exception e) {
            log.error("Error creating booking: {}", e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Get booking details
     *
     * @param bookingId the booking ID
     * @return ResponseEntity with the booking details
     */
    @GetMapping("/{bookingId}")
    public ResponseEntity<BookingDetails> getBooking(@PathVariable String bookingId) {
        log.info("Getting booking details: {}", bookingId);

        try {
            Booking booking = bookings.get(bookingId);

            if (booking == null) {
                log.warn("Booking not found: {}", bookingId);
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }

            BookingDetails details = new BookingDetails(
                booking.bookingId(),
                booking.userId(),
                booking.item(),
                booking.quantity(),
                booking.status(),
                booking.createdAt(),
                booking.updatedAt()
            );

            log.info("Retrieved booking details: {}", details);
            return new ResponseEntity<>(details, HttpStatus.OK);
        } catch (Exception e) {
            log.error("Error retrieving booking details: {}", e.getMessage(), e);
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
     * Internal entity for booking
     */
    private record Booking(
        String bookingId,
        String userId,
        String item,
        int quantity,
        String status,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
    ) {}
}
