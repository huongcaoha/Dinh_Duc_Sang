package ra.demo.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ra.demo.constants.VehicleType;
import ra.demo.dto.request.CalculatePriceRequest;
import ra.demo.dto.response.ApiResponse;
import ra.demo.dto.response.PriceCalculationResponse;
import ra.demo.service.PricingService;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/pricing")
@RequiredArgsConstructor
public class PricingController {
    private final PricingService pricingService;
    private final StringRedisTemplate redisTemplate;

    /**
     * 1. API Chơi thử tính cước chuyến đi
     * POST /api/v1/pricing/calculate
     */
    @PostMapping("/calculate")
    public ResponseEntity<ApiResponse<PriceCalculationResponse>> calculateFare(@RequestBody CalculatePriceRequest request) {
        PriceCalculationResponse response = pricingService.calculateFare(request);
        return new ResponseEntity<>(new ApiResponse<>(
                true,
                "Tính cước chuyến đi thành công",
                response,
                null,
                HttpStatus.OK
        ), HttpStatus.OK);
    }

    /**
     * 2. API Cấu hình Bảng giá vào REDIS (Dùng cho Admin/Test)
     * POST /api/v1/pricing/seed-rules
     */
    @PostMapping("/seed-rules")
    public ResponseEntity<ApiResponse<String>> seedPricingRulesToRedis() {
        // Nạp cấu hình bảng giá xe máy BIKE
        redisTemplate.opsForHash().put("pricing:rules:BIKE", "baseFare", "14000");  // 14k cho 2km đầu
        redisTemplate.opsForHash().put("pricing:rules:BIKE", "perKmRate", "5000");   // 5k/km tiếp theo

        // Nạp cấu hình bảng giá xe 4 chỗ CAR_4_SEAT
        redisTemplate.opsForHash().put("pricing:rules:CAR_4_SEAT", "baseFare", "18000");
        redisTemplate.opsForHash().put("pricing:rules:CAR_4_SEAT", "perKmRate", "12000");

        // Nạp cấu hình bảng giá xe 7 chỗ CAR_7_SEAT
        redisTemplate.opsForHash().put("pricing:rules:CAR_7_SEAT", "baseFare", "22000");
        redisTemplate.opsForHash().put("pricing:rules:CAR_7_SEAT", "perKmRate", "16000");

        return new ResponseEntity(new ApiResponse<>(
                true,
                "Đã nạp quy tắc giá mới vào Redis Hash thành công!",
                null,
                null,
                HttpStatus.OK
        ), HttpStatus.OK);
    }

    /**
     * 3. API Giả lập Tăng giá SURGE ZONE (Ví dụ khu vực Cầu Giấy 21.03_105.78 nhân giá 1.5x)
     * POST /api/v1/pricing/mock-surge?lat=21.0378&lng=105.7839&rate=1.5
     */
    @PostMapping("/mock-surge")
    public ResponseEntity<ApiResponse<String>> mockSurgeZone(
            @RequestParam Double lat,
            @RequestParam Double lng,
            @RequestParam String rate) {

        double truncatedLat = Math.floor(lat * 100.0) / 100.0;
        double truncatedLng = Math.floor(lng * 100.0) / 100.0;
        String zoneKey = String.format(java.util.Locale.US, "%.2f_%.2f", truncatedLat, truncatedLng);

        // Lưu hệ số tăng giá vào Redis với Key dạng surge:zone:21.03_105.78
        redisTemplate.opsForValue().set("surge:zone:" + zoneKey, rate);

        return new ResponseEntity(new ApiResponse<>(
                true,
                "Đã bật Surge Pricing " + rate + "x cho Zone " + zoneKey,
                null,
                null,
                HttpStatus.OK
        ), HttpStatus.OK);
    }

    /**
     * 4. API Xem các giá trị đang lưu trong Redis để kiểm tra
     * GET /api/v1/pricing/check-redis
     */
    @GetMapping("/check-redis")
    public ResponseEntity<ApiResponse<Map<String, Object>>> checkRedisData() {
        Map<String, Object> data = new HashMap<>();

        for (VehicleType type : VehicleType.values()) {
            Map<Object, Object> rule = redisTemplate.opsForHash().entries("pricing:rules:" + type.name());
            data.put("Rule_" + type.name(), rule);
        }

        return new ResponseEntity(new ApiResponse<>(
                true,
                "Lấy dữ liệu Redis thành công",
                data,
                null,
                HttpStatus.OK
        ), HttpStatus.OK);
    }
}
