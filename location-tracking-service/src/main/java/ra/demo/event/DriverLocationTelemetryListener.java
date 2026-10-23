package ra.demo.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ra.demo.constants.VehicleType;
import ra.demo.dto.request.DriverLocationRequest;
import ra.demo.dto.request.DriverLocationStreamDTO;
import ra.demo.service.LocationService;

@Component
@RequiredArgsConstructor
@Slf4j
public class DriverLocationTelemetryListener {

    private final LocationService locationService;

    /**
     * Lắng nghe tọa độ GPS liên tục (3s/lần) do websocket-gateway-service bắn sang
     */
    @KafkaListener(
            topics = "driver-location-telemetry",
            groupId = "location-tracking-group"
    )
    public void handleDriverLocationStream(DriverLocationStreamDTO dto) {
        if (dto == null || dto.getDriverId() == null) return;

        log.debug("📥 [KAFKA TELEMETRY] Nhận GPS Tài xế #{}: Lat={}, Lng={}",
                dto.getDriverId(), dto.getLat(), dto.getLng());

        // Chuyển đổi DTO từ Kafka sang Request DTO của LocationService
        DriverLocationRequest request = DriverLocationRequest.builder()
                .latitude(dto.getLat())
                .longitude(dto.getLng())
                .vehicleType(dto.getVehicleType() != null ? VehicleType.valueOf(dto.getVehicleType()) : VehicleType.BIKE)
                .build();

        // Gọi LocationService để cập nhật vào Redis Hash & Redis GEO
        locationService.updateDriverLocation(request);
    }
}
