package ch.ruyalabs.springkafkalab;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"payment-request-test"})
@ActiveProfiles("test")
class KafkaServiceApplicationTests {

    @Test
    void contextLoads() {
    }

}
