package ch.ruyalabs.springkafkalab.dto;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PaymentDtoTest {

    private PaymentDto paymentDto;

    @BeforeEach
    void setUp() {
        paymentDto = new PaymentDto();
    }

    @Test
    void testDefaultConstructor() {
        PaymentDto dto = new PaymentDto();
        assertNotNull(dto);
        assertNull(dto.getPaymentId());
        assertNull(dto.getAmount());
        assertNull(dto.getCurrency());
        assertNull(dto.getPaymentMethod());
        assertNull(dto.getStatus());
        assertNull(dto.getCreatedAt());
    }

    @Test
    void testParameterizedConstructor() {
        String paymentId = "payment-123";
        Double amount = 100.50;
        String currency = "USD";
        PaymentDto.PaymentMethodEnum paymentMethod = PaymentDto.PaymentMethodEnum.CREDIT_CARD;
        PaymentDto.StatusEnum status = PaymentDto.StatusEnum.PENDING;
        OffsetDateTime createdAt = OffsetDateTime.now();

        PaymentDto dto = new PaymentDto(paymentId, amount, currency, paymentMethod, status, createdAt);

        assertEquals(paymentId, dto.getPaymentId());
        assertEquals(amount, dto.getAmount());
        assertEquals(currency, dto.getCurrency());
        assertEquals(paymentMethod, dto.getPaymentMethod());
        assertEquals(status, dto.getStatus());
        assertEquals(createdAt, dto.getCreatedAt());
    }

    @Test
    void testSettersAndGetters() {
        String paymentId = "payment-456";
        Double amount = 250.75;
        String currency = "EUR";
        PaymentDto.PaymentMethodEnum paymentMethod = PaymentDto.PaymentMethodEnum.PAYPAL;
        PaymentDto.StatusEnum status = PaymentDto.StatusEnum.COMPLETED;
        OffsetDateTime createdAt = OffsetDateTime.now();
        OffsetDateTime updatedAt = OffsetDateTime.now();
        String description = "Test payment";
        String merchantId = "merchant-123";
        String customerId = "customer-456";
        String orderId = "order-789";
        Boolean isRecurring = true;
        String failureReason = "Insufficient funds";
        Double refundAmount = 25.0;

        paymentDto.setPaymentId(paymentId);
        paymentDto.setAmount(amount);
        paymentDto.setCurrency(currency);
        paymentDto.setPaymentMethod(paymentMethod);
        paymentDto.setStatus(status);
        paymentDto.setCreatedAt(createdAt);
        paymentDto.setUpdatedAt(updatedAt);
        paymentDto.setDescription(description);
        paymentDto.setMerchantId(merchantId);
        paymentDto.setCustomerId(customerId);
        paymentDto.setOrderId(orderId);
        paymentDto.setIsRecurring(isRecurring);
        paymentDto.setFailureReason(failureReason);
        paymentDto.setRefundAmount(refundAmount);

        assertEquals(paymentId, paymentDto.getPaymentId());
        assertEquals(amount, paymentDto.getAmount());
        assertEquals(currency, paymentDto.getCurrency());
        assertEquals(paymentMethod, paymentDto.getPaymentMethod());
        assertEquals(status, paymentDto.getStatus());
        assertEquals(createdAt, paymentDto.getCreatedAt());
        assertEquals(updatedAt, paymentDto.getUpdatedAt());
        assertEquals(description, paymentDto.getDescription());
        assertEquals(merchantId, paymentDto.getMerchantId());
        assertEquals(customerId, paymentDto.getCustomerId());
        assertEquals(orderId, paymentDto.getOrderId());
        assertEquals(isRecurring, paymentDto.getIsRecurring());
        assertEquals(failureReason, paymentDto.getFailureReason());
        assertEquals(refundAmount, paymentDto.getRefundAmount());
    }

    @Test
    void testFluentSetters() {
        String paymentId = "payment-fluent";
        Double amount = 150.25;
        String currency = "GBP";

        PaymentDto result = paymentDto
                .paymentId(paymentId)
                .amount(amount)
                .currency(currency);

        assertSame(paymentDto, result); // Should return the same instance
        assertEquals(paymentId, paymentDto.getPaymentId());
        assertEquals(amount, paymentDto.getAmount());
        assertEquals(currency, paymentDto.getCurrency());
    }

    @Test
    void testMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

        paymentDto.setMetadata(metadata);
        assertEquals(metadata, paymentDto.getMetadata());

        // Test fluent metadata setter
        PaymentDto result = paymentDto.metadata(metadata);
        assertSame(paymentDto, result);

        // Test putMetadataItem
        PaymentDto itemResult = paymentDto.putMetadataItem("key3", "value3");
        assertSame(paymentDto, itemResult);
        assertEquals("value3", paymentDto.getMetadata().get("key3"));
    }

    @Test
    void testEqualsAndHashCode() {
        PaymentDto dto1 = new PaymentDto();
        dto1.setPaymentId("payment-123");
        dto1.setAmount(100.0);
        dto1.setCurrency("USD");

        PaymentDto dto2 = new PaymentDto();
        dto2.setPaymentId("payment-123");
        dto2.setAmount(100.0);
        dto2.setCurrency("USD");

        PaymentDto dto3 = new PaymentDto();
        dto3.setPaymentId("payment-456");
        dto3.setAmount(200.0);
        dto3.setCurrency("EUR");

        assertEquals(dto1, dto2);
        assertEquals(dto1.hashCode(), dto2.hashCode());
        assertNotEquals(dto1, dto3);
        assertNotEquals(dto1.hashCode(), dto3.hashCode());
    }

    @Test
    void testToString() {
        paymentDto.setPaymentId("payment-123");
        paymentDto.setAmount(100.0);
        paymentDto.setCurrency("USD");

        String toString = paymentDto.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("PaymentDto"));
        assertTrue(toString.contains("payment-123"));
        assertTrue(toString.contains("100.0"));
        assertTrue(toString.contains("USD"));
    }

    @Test
    void testPaymentMethodEnum() {
        // Test all enum values
        PaymentDto.PaymentMethodEnum[] methods = PaymentDto.PaymentMethodEnum.values();
        assertTrue(methods.length > 0);

        for (PaymentDto.PaymentMethodEnum method : methods) {
            assertNotNull(method.getValue());
            assertNotNull(method.toString());
            assertEquals(method, PaymentDto.PaymentMethodEnum.fromValue(method.getValue()));
        }
    }

    @Test
    void testStatusEnum() {
        // Test all enum values
        PaymentDto.StatusEnum[] statuses = PaymentDto.StatusEnum.values();
        assertTrue(statuses.length > 0);

        for (PaymentDto.StatusEnum status : statuses) {
            assertNotNull(status.getValue());
            assertNotNull(status.toString());
            assertEquals(status, PaymentDto.StatusEnum.fromValue(status.getValue()));
        }
    }

    @Test
    void testEnumFromValueWithInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            PaymentDto.PaymentMethodEnum.fromValue("INVALID_METHOD");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            PaymentDto.StatusEnum.fromValue("INVALID_STATUS");
        });
    }
}