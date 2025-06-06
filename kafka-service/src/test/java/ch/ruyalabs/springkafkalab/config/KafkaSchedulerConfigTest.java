package ch.ruyalabs.springkafkalab.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaSchedulerConfigTest {

    private static final List<String> TEST_LISTENER_IDS = List.of("paymentRequestListener", "anotherListenerId");
    private static final ZoneId ZURICH_ZONE_ID = ZoneId.of("Europe/Zurich");

    @Mock
    private KafkaListenerEndpointRegistry registry;

    private Map<String, MessageListenerContainer> mockContainers;

    private KafkaSchedulerConfig kafkaSchedulerConfig;

    @BeforeEach
    void setUp() {
        // Create a map of listener IDs to mock containers for verification purposes
        mockContainers = TEST_LISTENER_IDS.stream()
                .collect(Collectors.toMap(Function.identity(), id -> mock(MessageListenerContainer.class)));

        // The KafkaSchedulerConfig instance is created with the list of IDs
        kafkaSchedulerConfig = new KafkaSchedulerConfig(registry, TEST_LISTENER_IDS);
    }

    /**
     * Helper method to set up mock stubs for the registry.
     * This is called only by tests that need this specific behavior.
     */
    private void setupRegistryMocks() {
        mockContainers.forEach((id, container) ->
                when(registry.getListenerContainer(id)).thenReturn(container));
    }

    @Test
    void pauseConsumers_shouldPauseAllRegisteredContainers() {
        // Arrange
        setupRegistryMocks(); // Stubbing is now local to the test
        mockContainers.values().forEach(container -> when(container.isContainerPaused()).thenReturn(false));

        // Act
        kafkaSchedulerConfig.pauseConsumers();

        // Assert
        verify(registry, times(TEST_LISTENER_IDS.size())).getListenerContainer(anyString());
        mockContainers.values().forEach(container -> {
            verify(container).pause();
            verify(container, never()).resume();
        });
    }

    @Test
    void resumeConsumers_shouldResumeAllRegisteredContainers() {
        // Arrange
        setupRegistryMocks(); // Stubbing is now local to the test
        mockContainers.values().forEach(container -> when(container.isContainerPaused()).thenReturn(true));

        // Act
        kafkaSchedulerConfig.resumeConsumers();

        // Assert
        verify(registry, times(TEST_LISTENER_IDS.size())).getListenerContainer(anyString());
        mockContainers.values().forEach(container -> {
            verify(container).resume();
            verify(container, never()).pause();
        });
    }

    @Test
    void startupPauser_shouldPauseAllConsumers_whenStartedDuringMaintenanceWindow() throws Exception {
        // Arrange
        setupRegistryMocks(); // Stubbing is now local to the test
        LocalDateTime maintenanceTime = LocalDateTime.of(2025, 7, 1, 3, 30); // Using a future date
        try (MockedStatic<LocalDateTime> mockedStatic = mockStatic(LocalDateTime.class)) {
            mockedStatic.when(() -> LocalDateTime.now(ZURICH_ZONE_ID)).thenReturn(maintenanceTime);

            // Act
            ApplicationRunner runner = kafkaSchedulerConfig.startupPauser();
            runner.run(null);

            // Assert
            mockContainers.values().forEach(container -> verify(container).pause());
        }
    }

    @Test
    void startupPauser_shouldNotPauseAnyConsumer_whenStartedOutsideMaintenanceWindow() throws Exception {
        // Arrange
        // NO stubbing of the registry is performed here, which fixes the exception.
        LocalDateTime normalTime = LocalDateTime.of(2025, 7, 2, 10, 0); // Using a future date
        try (MockedStatic<LocalDateTime> mockedStatic = mockStatic(LocalDateTime.class)) {
            mockedStatic.when(() -> LocalDateTime.now(ZURICH_ZONE_ID)).thenReturn(normalTime);

            // Act
            ApplicationRunner runner = kafkaSchedulerConfig.startupPauser();
            runner.run(null);

            // Assert
            // Verify that the registry was not interacted with at all.
            verifyNoInteractions(registry);
            mockContainers.values().forEach(container -> verify(container, never()).pause());
        }
    }
}