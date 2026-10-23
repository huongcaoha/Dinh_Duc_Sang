package ra.demo.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import ra.demo.dto.request.DriverLocationStreamDTO;

@Controller
@RequiredArgsConstructor
@Slf4j
public class LocationWebSocketController {

    private final SimpMessagingTemplate messagingTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    // Tài xế stream GPS liên tục (3-5s/lần) lên endpoint STOMP: /app/location/stream
    @MessageMapping("/location/stream")
    public void receiveDriverLocation(DriverLocationStreamDTO dto) {
        log.debug("📡 [GPS STREAM] Tài xế #{}: Lat={}, Lng={}, Vehicle={}",
                dto.getDriverId(), dto.getLat(), dto.getLng(), dto.getVehicleType());

        // 1. Theo sơ đồ: Stream Location được đẩy về 'location-tracking-service' để cập nhật Redis GEO
        // Bắn tọa độ qua Kafka Topic "driver-location-telemetry" để Location Service tiêu thụ
        kafkaTemplate.send("driver-location-telemetry", String.valueOf(dto.getDriverId()), dto);

        // 2. Nếu tài xế đang trong chuyến đi (tripId != null), forward vị trí realtime cho Khách hàng theo dõi trên bản đồ
        if (dto.getTripId() != null) {
            messagingTemplate.convertAndSend("/topic/trip/" + dto.getTripId() + "/location", dto);
        }
    }
}
