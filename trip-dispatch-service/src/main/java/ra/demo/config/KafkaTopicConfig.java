package ra.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    public static final String RIDE_REQUEST_TOPIC = "trip-dispatch-topic";

    @Bean
    public NewTopic rideRequestTopic() {
        return TopicBuilder.name(RIDE_REQUEST_TOPIC)
                .partitions(3) // Chia 3 partition để xử lý đa luồng
                .replicas(1)
                .build();
    }
}
