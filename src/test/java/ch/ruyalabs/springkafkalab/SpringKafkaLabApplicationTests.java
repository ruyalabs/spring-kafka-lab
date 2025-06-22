package ch.ruyalabs.springkafkalab;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@EmbeddedKafka(partitions = 1, 
               topics = {"payment-request-test", "payment-response-test"},
               brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@ActiveProfiles("test")
class SpringKafkaLabApplicationTests {

	@Test
	void contextLoads() {
		// This test verifies that the Spring application context loads successfully
		// with the structured logging configuration
	}

}
