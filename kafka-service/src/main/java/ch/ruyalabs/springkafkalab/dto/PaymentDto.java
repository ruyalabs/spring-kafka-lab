package ch.ruyalabs.springkafkalab.dto;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentDto {
    private String id;
    private BigDecimal amount;
    private String currency;
    private String description;
}
