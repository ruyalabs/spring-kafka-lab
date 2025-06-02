package ch.ruyalabs.springkafkalab.controller;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.service.PaymentRequestProcessorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentRequestProcessorService paymentRequestProcessorService;

    /**
     * Process a payment request
     * 
     * @param paymentDto the payment request to process
     * @return ResponseEntity with the processed payment
     */
    @PostMapping
    public ResponseEntity<PaymentDto> processPayment(@RequestBody PaymentDto paymentDto) {
        log.info("Received payment request: {}", paymentDto);

        try {
            PaymentDto processedPayment = paymentRequestProcessorService.processPayment(paymentDto);
            return new ResponseEntity<>(processedPayment, HttpStatus.CREATED);
        } catch (Exception e) {
            log.error("Error processing payment: {}", e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
