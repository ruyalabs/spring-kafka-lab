package ch.ruyalabs.springkafkalab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SpringKafkaLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaLabApplication.class, args);
    }
}
