package ch.ruyalabs.springkafkalab.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration for RestTemplate
 */
@Configuration
public class RestTemplateConfig {

    /**
     * Create a RestTemplate bean
     *
     * @return the RestTemplate bean
     */
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}