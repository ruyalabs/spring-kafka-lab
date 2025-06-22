package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"payment-request"})
@DirtiesContext
class PaymentRequestConsumerTest {

    @InjectMocks
    private PaymentRequestConsumer paymentRequestConsumer;

    private PaymentDto validPaymentDto;

    @BeforeEach
    void setUp() {
        // Create a valid PaymentDto for testing
        validPaymentDto = new PaymentDto();
        validPaymentDto.setPaymentId("payment-123");
        validPaymentDto.setAmount(100.50);
        validPaymentDto.setCurrency("USD");
        validPaymentDto.setPaymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);
        validPaymentDto.setStatus(PaymentDto.StatusEnum.PENDING);
        validPaymentDto.setCreatedAt(OffsetDateTime.now());
        validPaymentDto.setCustomerId("customer-456");
        validPaymentDto.setMerchantId("merchant-789");
        validPaymentDto.setDescription("Test payment");
    }

    @Test
    void testConsumeValidPaymentDto() {
        // Test that the consumer can process a valid PaymentDto without throwing exceptions
        assertDoesNotThrow(() -> {
            paymentRequestConsumer.consume(validPaymentDto);
        });
    }

    @Test
    void testConsumeWithNullPaymentDto() {
        // Test behavior when null PaymentDto is passed
        assertDoesNotThrow(() -> {
            paymentRequestConsumer.consume(null);
        });
    }

    @Test
    void testConsumeWithMinimalPaymentDto() {
        // Test with minimal PaymentDto (only required fields)
        PaymentDto minimalDto = new PaymentDto();
        minimalDto.setPaymentId("minimal-payment");
        minimalDto.setAmount(50.0);
        minimalDto.setCurrency("EUR");
        minimalDto.setPaymentMethod(PaymentDto.PaymentMethodEnum.BANK_TRANSFER);
        minimalDto.setStatus(PaymentDto.StatusEnum.COMPLETED);

        assertDoesNotThrow(() -> {
            paymentRequestConsumer.consume(minimalDto);
        });
    }

    @Test
    void testConsumeWithAllFieldsPopulated() {
        // Test with PaymentDto having all fields populated
        PaymentDto fullDto = new PaymentDto();
        fullDto.setPaymentId("full-payment-123");
        fullDto.setAmount(250.75);
        fullDto.setCurrency("GBP");
        fullDto.setPaymentMethod(PaymentDto.PaymentMethodEnum.PAYPAL);
        fullDto.setStatus(PaymentDto.StatusEnum.FAILED);
        fullDto.setCreatedAt(OffsetDateTime.now());
        fullDto.setUpdatedAt(OffsetDateTime.now());
        fullDto.setCustomerId("customer-full-123");
        fullDto.setMerchantId("merchant-full-456");
        fullDto.setOrderId("order-789");
        fullDto.setDescription("Full payment test");
        fullDto.setIsRecurring(true);
        fullDto.setFailureReason("Insufficient funds");
        fullDto.setRefundAmount(25.0);

        assertDoesNotThrow(() -> {
            paymentRequestConsumer.consume(fullDto);
        });
    }

    @Test
    void testConsumeWithDifferentPaymentMethods() {
        // Test consumption with different payment methods
        for (PaymentDto.PaymentMethodEnum method : PaymentDto.PaymentMethodEnum.values()) {
            PaymentDto dto = new PaymentDto();
            dto.setPaymentId("payment-" + method.getValue());
            dto.setAmount(100.0);
            dto.setCurrency("USD");
            dto.setPaymentMethod(method);
            dto.setStatus(PaymentDto.StatusEnum.PENDING);

            assertDoesNotThrow(() -> {
                paymentRequestConsumer.consume(dto);
            }, "Should handle payment method: " + method.getValue());
        }
    }

    @Test
    void testConsumeWithDifferentStatuses() {
        // Test consumption with different payment statuses
        for (PaymentDto.StatusEnum status : PaymentDto.StatusEnum.values()) {
            PaymentDto dto = new PaymentDto();
            dto.setPaymentId("payment-" + status.getValue());
            dto.setAmount(100.0);
            dto.setCurrency("USD");
            dto.setPaymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);
            dto.setStatus(status);

            assertDoesNotThrow(() -> {
                paymentRequestConsumer.consume(dto);
            }, "Should handle status: " + status.getValue());
        }
    }

    @Test
    void testConsumeWithZeroAmount() {
        // Test with zero amount
        PaymentDto zeroAmountDto = new PaymentDto();
        zeroAmountDto.setPaymentId("zero-payment");
        zeroAmountDto.setAmount(0.0);
        zeroAmountDto.setCurrency("USD");
        zeroAmountDto.setPaymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);
        zeroAmountDto.setStatus(PaymentDto.StatusEnum.PENDING);

        assertDoesNotThrow(() -> {
            paymentRequestConsumer.consume(zeroAmountDto);
        });
    }

    @Test
    void testConsumeWithNegativeAmount() {
        // Test with negative amount (refund scenario)
        PaymentDto negativeAmountDto = new PaymentDto();
        negativeAmountDto.setPaymentId("refund-payment");
        negativeAmountDto.setAmount(-50.0);
        negativeAmountDto.setCurrency("USD");
        negativeAmountDto.setPaymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);
        negativeAmountDto.setStatus(PaymentDto.StatusEnum.REFUNDED);

        assertDoesNotThrow(() -> {
            paymentRequestConsumer.consume(negativeAmountDto);
        });
    }

    @Test
    void testConsumeWithLargeAmount() {
        // Test with large amount
        PaymentDto largeAmountDto = new PaymentDto();
        largeAmountDto.setPaymentId("large-payment");
        largeAmountDto.setAmount(999999.99);
        largeAmountDto.setCurrency("USD");
        largeAmountDto.setPaymentMethod(PaymentDto.PaymentMethodEnum.BANK_TRANSFER);
        largeAmountDto.setStatus(PaymentDto.StatusEnum.PENDING);

        assertDoesNotThrow(() -> {
            paymentRequestConsumer.consume(largeAmountDto);
        });
    }

    @Test
    void testConsumeWithDifferentCurrencies() {
        // Test with different currencies
        String[] currencies = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD"};

        for (String currency : currencies) {
            PaymentDto dto = new PaymentDto();
            dto.setPaymentId("payment-" + currency);
            dto.setAmount(100.0);
            dto.setCurrency(currency);
            dto.setPaymentMethod(PaymentDto.PaymentMethodEnum.CREDIT_CARD);
            dto.setStatus(PaymentDto.StatusEnum.PENDING);

            assertDoesNotThrow(() -> {
                paymentRequestConsumer.consume(dto);
            }, "Should handle currency: " + currency);
        }
    }
}
