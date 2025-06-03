package ch.ruyalabs.springkafkalab.controller;

import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.InsufficientFundsException;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.service.PaymentRequestProcessorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class PaymentControllerTest {

    @Mock
    private PaymentRequestProcessorService paymentRequestProcessorService;

    @InjectMocks
    private PaymentController paymentController;

    private MockMvc mockMvc;

    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(paymentController).build();
    }

    @Test
    void processPayment_withValidPayment_shouldReturnCreatedStatus() throws Exception {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", "Test payment");

        when(paymentRequestProcessorService.processPayment(any(PaymentDto.class))).thenReturn(paymentDto);

        // Act & Assert
        mockMvc.perform(post("/api/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(paymentDto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(paymentDto.getId()))
                .andExpect(jsonPath("$.amount").value(paymentDto.getAmount().doubleValue()))
                .andExpect(jsonPath("$.currency").value(paymentDto.getCurrency()))
                .andExpect(jsonPath("$.description").value(paymentDto.getDescription()));
    }

    @Test
    void processPayment_withGeneratedId_shouldReturnCreatedStatus() throws Exception {
        // Arrange
        PaymentDto requestDto = new PaymentDto(null, new BigDecimal("100.00"), "USD", "Test payment");
        PaymentDto responseDto = new PaymentDto("generated-id", new BigDecimal("100.00"), "USD", "Test payment");

        when(paymentRequestProcessorService.processPayment(any(PaymentDto.class))).thenReturn(responseDto);

        // Act & Assert
        mockMvc.perform(post("/api/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(requestDto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(responseDto.getId()))
                .andExpect(jsonPath("$.amount").value(responseDto.getAmount().doubleValue()))
                .andExpect(jsonPath("$.currency").value(responseDto.getCurrency()))
                .andExpect(jsonPath("$.description").value(responseDto.getDescription()));
    }

    @Test
    void processPayment_withInvalidPayment_shouldReturnInternalServerError() throws Exception {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("-100.00"), "USD", "Test payment");

        when(paymentRequestProcessorService.processPayment(any(PaymentDto.class)))
                .thenThrow(new IllegalArgumentException("Payment amount must be positive"));

        // Act & Assert
        mockMvc.perform(post("/api/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(paymentDto)))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void processPayment_withInsufficientFunds_shouldReturnInternalServerError() throws Exception {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("1000.00"), "USD", "Test payment");

        when(paymentRequestProcessorService.processPayment(any(PaymentDto.class)))
                .thenThrow(new InsufficientFundsException("Insufficient funds for payment"));

        // Act & Assert
        mockMvc.perform(post("/api/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(paymentDto)))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void processPayment_withRuntimeException_shouldReturnInternalServerError() throws Exception {
        // Arrange
        PaymentDto paymentDto = new PaymentDto("payment-123", new BigDecimal("100.00"), "USD", "Test payment");

        when(paymentRequestProcessorService.processPayment(any(PaymentDto.class)))
                .thenThrow(new RuntimeException("Unexpected error"));

        // Act & Assert
        mockMvc.perform(post("/api/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(paymentDto)))
                .andExpect(status().isInternalServerError());
    }
}
