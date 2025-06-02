package ch.ruyalabs.bookingservice.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/bookings")
public class BookingController {

    @PostMapping
    public BookingResponse createBooking(@RequestBody BookingRequest request) {
        // Dummy implementation
        return new BookingResponse("booking-" + System.currentTimeMillis(), request.item(), "CONFIRMED");
    }

    // Dummy DTOs
    record BookingRequest(String userId, String item, int quantity) {}
    record BookingResponse(String bookingId, String item, String status) {}
}