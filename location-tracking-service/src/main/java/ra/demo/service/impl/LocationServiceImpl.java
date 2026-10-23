package ra.demo.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import ra.demo.constants.VehicleType;
import ra.demo.dto.request.DriverLocationRequest;
import ra.demo.dto.response.NearbyDriverResponse;
import ra.demo.security.utils.SecurityUtils;
import ra.demo.service.LocationService;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class LocationServiceImpl implements LocationService {
    private final StringRedisTemplate redisTemplate;

    /**
     * HÀM 1: Cập nhật GPS liên tục do App Tài xế bắn về (Thường 3s - 5s gửi 1 lần)
     */
    @Override
    public void updateDriverLocation(DriverLocationRequest request) {
        // Lấy ID của tài xế đang thực hiện request (Trích xuất từ Token JWT)
        Long driverId = SecurityUtils.getCurrentUserId();

        if (driverId == null) {
            log.warn("[LOCATION UPDATE WARNING] Không tìm thấy driverId trong Security Context");
            return;
        }

        String driverIdStr = String.valueOf(driverId);
        VehicleType vehicleType = request.getVehicleType() != null ? request.getVehicleType() : VehicleType.BIKE;

        // BƯỚC 1: Lưu Lịch Sử Vị Trí Mới Nhất (Dạng Redis Hash: "driver:location:100")
        // Giống như việc ghi nhật ký: Tài xế 100 đang ở Lat=..., Lng=..., Đi xe=BIKE
        String lastLocationKey = "driver:location:" + driverIdStr;
        redisTemplate.opsForHash().put(lastLocationKey, "lat", String.valueOf(request.getLatitude()));
        redisTemplate.opsForHash().put(lastLocationKey, "lng", String.valueOf(request.getLongitude()));
        redisTemplate.opsForHash().put(lastLocationKey, "vehicleType", vehicleType.name()); // Thêm dòng này

        // BƯỚC 2: Kiểm tra xem Tài xế này đang "ONLINE" hay "OFFLINE"
        String status = redisTemplate.opsForValue().get("driver:status:" + driverIdStr);
        String geoKey = "drivers:available:" + vehicleType.name();

        // Nếu ĐANG ONLINE -> Cập nhật vị trí GPS vào Bản đồ Xe Rảnh (Redis GEO)
        if ("ONLINE".equalsIgnoreCase(status)) {
            redisTemplate.opsForGeo().add(
                    geoKey,
                    new Point(request.getLongitude(), request.getLatitude()),
                    driverIdStr
            );
            log.debug("[LOCATION ONLINE] Cập nhật GPS Tài xế #{}: ({}, {})", driverId, request.getLatitude(), request.getLongitude());
        }
        // Nếu ĐANG OFFLINE mà vẫn cố bắn GPS -> Xóa luôn khỏi Bản đồ Xe Rảnh
        else {
            redisTemplate.opsForZSet().remove(geoKey, driverIdStr);
            log.debug("[LOCATION OFFLINE] Tài xế #{} đang Offline. Xóa khỏi {}", driverId, geoKey);
        }
    }


    /**
     * HÀM 2: Tìm kiếm các tài xế xung quanh vị trí của Khách hàng (Spatial Query)
     */
    @Override
    public List<NearbyDriverResponse> findNearbyDrivers(Double lat, Double lng, Double radiusKm, String vehicleType) {
        // Mặc định nếu không chọn loại xe thì lấy Xe Máy (BIKE)
        String targetVehicleType = (vehicleType != null && !vehicleType.isBlank()) ? vehicleType : "BIKE";
        String geoKey = "drivers:available:" + targetVehicleType;

        // Vẽ một hình tròn quanh vị trí Khách hàng (Tọa độ Lat/Lng của khách, Bán kính X km)
        Circle searchArea = new Circle(
                new Point(lng, lat),
                new Distance(radiusKm, Metrics.KILOMETERS)
        );

        // Cấu hình kết quả trả về: Kèm khoảng cách, Kèm tọa độ, Sắp xếp từ GẦN NHẤT đến XA NHẤT
        RedisGeoCommands.GeoRadiusCommandArgs args = RedisGeoCommands.GeoRadiusCommandArgs
                .newGeoRadiusArgs()
                .includeDistance() // Khoảng cách
                .includeCoordinates() // Toạ độ
                .sortAscending(); // Ưu tiên tài xế gần nhất

        // Bắt đầu "Quét" Redis GEO trong hình tròn đó!
        GeoResults<RedisGeoCommands.GeoLocation<String>> results =
                redisTemplate.opsForGeo().radius(geoKey, searchArea, args);

        List<NearbyDriverResponse> nearbyDrivers = new ArrayList<>();

        // Lặp qua từng tài xế tìm thấy được
        if (results != null) {
            for (GeoResult<RedisGeoCommands.GeoLocation<String>> result : results) {
                String driverIdStr = result.getContent().getName();

                if (driverIdStr == null || driverIdStr.isBlank() || "null".equalsIgnoreCase(driverIdStr.trim())) {
                    log.warn("[SPATIAL QUERY WARNING] Phát hiện memberName không hợp lệ trong Redis GEO: '{}'", driverIdStr);
                    continue;
                }

                try {
                    // Làm sạch ID tài xế (Lấy con số ID)
                    String cleanDriverIdStr = driverIdStr.contains(":")
                            ? driverIdStr.substring(driverIdStr.lastIndexOf(":") + 1)
                            : driverIdStr;

                    Long driverId = Long.parseLong(cleanDriverIdStr.trim());
                    Point point = result.getContent().getPoint();
                    Double distance = result.getDistance().getValue();

                    // Đóng gói thông tin tài xế (ID, Vĩ độ, Kinh độ, Khoảng cách km) gửi lại cho Client
                    nearbyDrivers.add(NearbyDriverResponse.builder()
                            .driverId(driverId)
                            .latitude(point != null ? point.getY() : null)
                            .longitude(point != null ? point.getX() : null)
                            .distanceKm(Math.round(distance * 100.0) / 100.0)
                            .vehicleType(VehicleType.valueOf(targetVehicleType))
                            .build());

                } catch (Exception e) {
                    log.error("[SPATIAL QUERY ERROR] Không thể parse driverId từ chuỗi: '{}'", driverIdStr, e);
                }
            }
        }

        log.info("[SPATIAL QUERY] Tìm thấy {} tài xế loại [{}] xung quanh bán kính {} km",
                nearbyDrivers.size(), targetVehicleType, radiusKm);
        return nearbyDrivers;
    }

    /**
     * HÀM 3: Xóa tài xế khỏi toàn bộ danh sách xe rảnh (Khi bận chở khách hoặc Offline)
     */
    @Override
    public void removeDriverLocation(Long driverId) {
        String driverIdStr = String.valueOf(driverId);

        // Duyệt qua tất cả các key loại xe và xóa ID tài xế này ra khỏi Redis ZSet/GEO
        for (VehicleType type : VehicleType.values()) {
            String geoKey = "drivers:available:" + type.name();
            redisTemplate.opsForZSet().remove(geoKey, driverIdStr);
        }

        log.info("[LOCATION REMOVE] Đã xóa Tài xế #{} khỏi toàn bộ danh sách xe rảnh", driverId);
    }
}
