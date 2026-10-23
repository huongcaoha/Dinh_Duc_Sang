package ra.demo.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RideRequestKafkaConsumer {

    private final SimpMessagingTemplate messagingTemplate;

    // Lắng nghe Event nổ chuyến 30s được bắn từ Trip Service qua Kafka
    @KafkaListener(
            topics = "ride-request-topic",
            groupId = "websocket-gateway-group"
    )
    public void handleRideRequest(RideRequestEvent event) {
        log.info("🔔 [KAFKA -> WEBSOCKET] Nhận sự kiện nổ chuyến #{} từ Kafka", event.getTripId());

        if (event.getCandidateDriverIds() != null) {
            for (String driverId : event.getCandidateDriverIds()) {
                // Channel riêng cho tài xế nhận chuyến đếm ngược 30s
                String destination = "/topic/driver/" + driverId + "/ride-request";

                // Push thông báo xuống App tài xế qua WebSocket
                messagingTemplate.convertAndSend(destination, event);

                log.info("⚡ [PUSH SUCCESS] Đã đẩy thông báo chuyến đi #{} tới Tài xế #{} qua {}",
                        event.getTripId(), driverId, destination);
            }
        }
    }
}