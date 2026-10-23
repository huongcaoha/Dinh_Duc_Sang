package ra.demo.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ra.demo.constants.DriverStatus;
import ra.demo.constants.VehicleType;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class DriverStatusEventListener {
    // Khai báo công cụ làm việc với Redis
    private final StringRedisTemplate redisTemplate;

    /**
     * @KafkaListener: Hàm này giống như người đứng gác.
     * Hễ có tin nhắn gửi vào loa thông báo "driver-status-topic" (do User Service phát ra),
     * hàm này sẽ tự động nhảy vào xử lý!
     */
    @KafkaListener(topics = "driver-status-topic", groupId = "location-tracking-group")
    public void handleDriverStatusChange(DriverStatusEvent event) {
        // Nếu tin nhắn rỗng hoặc không có ID tài xế thì bỏ qua, không làm gì cả
        if (event == null || event.getDriverId() == null) return;

        String driverIdStr = String.valueOf(event.getDriverId());
        String redisKey = "driver:status:" + event.getDriverId();
        // Lấy tên trạng thái ("ONLINE" hoặc "OFFLINE")
        String statusStr = event.getStatus() != null ? event.getStatus().name() : "OFFLINE";

        // BƯỚC 1: Dán nhãn trạng thái của Tài xế vào Redis (Ví dụ: "driver:status:100" -> "ONLINE")
        redisTemplate.opsForValue().set(redisKey, statusStr);
        log.info("[KAFKA CONSUME] Đã cập nhật trạng thái Redis Cache cho Tài xế #{}: {}", driverIdStr, statusStr);

        // BƯỚC 2: Nếu tài xế vừa bấm "BẬT ONLINE"
        if ("ONLINE".equalsIgnoreCase(statusStr)) {
            // Mở sổ ra xem vị trí GPS gần đây nhất của tài xế này đang ở đâu
            String lastLocationKey = "driver:location:" + driverIdStr;
            Map<Object, Object> locationData = redisTemplate.opsForHash().entries(lastLocationKey);

            // Nếu trong sổ đã có lưu Tọa độ (Lat, Lng) và Loại xe (BIKE/CAR)
            if (locationData.containsKey("lat") && locationData.containsKey("lng") && locationData.containsKey("vehicleType")) {
                double lat = Double.parseDouble(locationData.get("lat").toString());
                double lng = Double.parseDouble(locationData.get("lng").toString());
                String vehicleTypeStr = locationData.get("vehicleType").toString();

                // Đưa vị trí tài xế này vào "Bản đồ xe rảnh" (Redis GEO) để sẵn sàng đón khách
                String geoKey = "drivers:available:" + vehicleTypeStr;
                redisTemplate.opsForGeo().add(geoKey, new Point(lng, lat), driverIdStr);
                log.info("[KAFKA ONLINE AUTO-GEO] Đã thêm Tài xế #{} vào GEO [{}] ngay khi bật ONLINE!", driverIdStr, geoKey);
            }
        }
        // BƯỚC 3: Nếu tài xế bấm "TẮT OFFLINE" (Nghỉ làm)
        else if ("OFFLINE".equalsIgnoreCase(statusStr)) {
            // Quét qua tất cả loại xe (Xe máy, Xe 4 chỗ, Xe 7 chỗ) và XÓA TÊN tài xế này khỏi danh sách chờ đón khách!
            for (VehicleType type : VehicleType.values()) {
                String geoKey = "drivers:available:" + type.name();
                redisTemplate.opsForZSet().remove(geoKey, driverIdStr);
            }
            log.info("[KAFKA REMOVE] Tài xế #{} chuyển OFFLINE. Đã xóa khỏi danh sách xe rảnh!", driverIdStr);
        }

    }
}