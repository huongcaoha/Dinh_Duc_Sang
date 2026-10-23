package ra.demo.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ra.demo.client.PricingClient;
import ra.demo.config.KafkaTopicConfig;
import ra.demo.constants.DriverStatus;
import ra.demo.constants.TripStatus;
import ra.demo.dto.request.CalculatePriceRequest;
import ra.demo.dto.request.CreateTripRequest;
import ra.demo.dto.response.ApiResponse;
import ra.demo.dto.response.PriceCalculationResponse;
import ra.demo.dto.response.TripResponse;
import ra.demo.entity.Trip;
import ra.demo.event.DriverStatusEvent;
import ra.demo.event.RideRequestEvent;
import ra.demo.event.TripStatusEvent;
import ra.demo.exception.TripException;
import ra.demo.repository.TripRepository;
import ra.demo.security.utils.SecurityUtils;
import ra.demo.service.TripService;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class TripServiceImpl implements TripService {
    private final TripRepository tripRepository;
    private final PricingClient pricingClient; // Tiêm Feign Client gọi Pricing Service
    private final KafkaTemplate<String, Object> kafkaTemplate; // Tiêm KafkaTemplate để bắn Event
    private final StringRedisTemplate redisTemplate;

    private static final Set<TripStatus> ACTIVE_STATUSES = Set.of(
            TripStatus.Requested,
            TripStatus.Accepted,
            TripStatus.Arrived,
            TripStatus.InProgress
    );

    // CUSTOMER (Khách hàng)
    @Override
    public TripResponse createTrip(CreateTripRequest createTripRequest) {
        Long customerId = SecurityUtils.getCurrentUserId();

        // 1. Check khách có chuyến chưa xong không
        if (tripRepository.existsByCustomerIdAndStatusIn(customerId, ACTIVE_STATUSES)) {
            log.warn("[TẠO CHUYẾN THẤT BẠI] Khách #{}: Đang có chuyến đi chưa hoàn thành", customerId);
            throw new TripException("Bạn đang có một chuyến đi chưa hoàn thành. Không thể đặt thêm!");

        }

        // 2. GỌI FEIGN CLIENT SANG PRICING SERVICE ĐỂ TÍNH TIỀN & LẤY TỌA ĐỘ
        CalculatePriceRequest priceRequest = CalculatePriceRequest.builder()
                .pickupAddress(createTripRequest.getPickupAddress())
                .dropoffAddress(createTripRequest.getDropoffAddress())
                .vehicleType(createTripRequest.getVehicleType())
                .build();

        log.info("[FEIGN CALL] Đang gọi Pricing Service tính tiền cho khách #{}", customerId);
        ApiResponse<PriceCalculationResponse> pricingApiResponse = pricingClient.calculateFare(priceRequest);

        if (pricingApiResponse == null || pricingApiResponse.getData() == null) {
            throw new TripException("Không thể tính giá cước chuyến đi lúc này. Vui lòng thử lại sau!");
        }

        PriceCalculationResponse fareData = pricingApiResponse.getData();

        // 3. KHỞI TẠO VÀ LƯU ENTITY TRIP VÀO DATABASE TRIP SERVICE
        Trip trip = Trip.builder()
                .customerId(customerId)
                .pickupAddress(createTripRequest.getPickupAddress())
                .dropoffAddress(createTripRequest.getDropoffAddress())
                .vehicleType(createTripRequest.getVehicleType())
                .distanceKm(fareData.getDistanceKm())
                .durationMinutes(fareData.getDurationMinutes())
                .fareAmount(fareData.getFareAmount())        // Giá tiền nhận từ Pricing Service
                .paymentMethod(createTripRequest.getPaymentMethod())
                .paymentStatus("UNPAID")
                .status(TripStatus.Requested)               // Trạng thái ban đầu: Requested
                .build();

        Trip savedTrip = tripRepository.save(trip);
        log.info("[TẠO CHUYẾN THÀNH CÔNG] Trip #{} - Tổng tiền: {} VNĐ", savedTrip.getId(), savedTrip.getFareAmount());

        // 4. PHÁT EVENT SANG KAFKA TOPIC "trip-dispatch-topic"
        // Lấy tọa độ GPS chuẩn (đã được Pricing Service Geocode) để gắn vào Event
        RideRequestEvent event = RideRequestEvent.builder()
                .tripId(savedTrip.getId())
                .customerId(savedTrip.getCustomerId())
                .pickupLat(fareData.getPickupLat())
                .pickupLng(fareData.getPickupLng())
                .pickupAddress(savedTrip.getPickupAddress())
                .dropoffLat(fareData.getDropoffLat())
                .dropoffLng(fareData.getDropoffLng())
                .dropoffAddress(savedTrip.getDropoffAddress())
                .amount(savedTrip.getFareAmount())
                .vehicleType(savedTrip.getVehicleType().name())
                .paymentMethod(savedTrip.getPaymentMethod().name())
                .build();

        kafkaTemplate.send(KafkaTopicConfig.RIDE_REQUEST_TOPIC, String.valueOf(savedTrip.getId()), event);
        log.info("[KAFKA PUBLISH] Đã gửi RideRequestEvent cho Trip #{} lên topic '{}'",
                savedTrip.getId(), KafkaTopicConfig.RIDE_REQUEST_TOPIC);

        // 5. Trả kết quả Response về cho Client
        return mapToTripResponse(savedTrip);
    }

    @Override
    public TripResponse getCustomerMyTrip(Long tripId) {
        Long customerId = SecurityUtils.getCurrentUserId();
        Trip trip = tripRepository.findByIdAndCustomerId(tripId, customerId)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy chuyến đi #" + tripId + " của bạn!"));
        return mapToTripResponse(trip);
    }

    @Override
    public List<TripResponse> getCustomerTripHistory() {
        Long customerId = SecurityUtils.getCurrentUserId();
        List<Trip> trips = tripRepository.findByCustomerIdOrderByCreatedAtDesc(customerId);
        return trips.stream().map(this::mapToTripResponse).collect(Collectors.toList());
    }

    @Override
    @Transactional
    public TripResponse cancelTripByCustomer(Long tripId, String cancelReason) {
        Long customerId = SecurityUtils.getCurrentUserId();
        Trip trip = tripRepository.findByIdAndCustomerId(tripId, customerId)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy chuyến đi #" + tripId));

        // Chỉ cho phép hủy khi đang ở trạng thái REQUESTED hoặc ACCEPTED
        if (trip.getStatus() == TripStatus.Completed || trip.getStatus() == TripStatus.Cancelled) {
            throw new TripException("Chuyến đi đã kết thúc hoặc đã bị hủy trước đó!");
        }

        if (trip.getStatus() == TripStatus.InProgress) {
            throw new TripException("Chuyến đi đang thực hiện (In Progress), không thể hủy!");
        }

        // Nếu khách hủy khi tài xế đã nhận chuyến -> Giải phóng tài xế về ONLINE
        if (trip.getDriverId() != null) {
            publishDriverStatusEvent(trip.getDriverId(), DriverStatus.ONLINE);
        }

        // Xóa triệt để các Key Redis Queue/Offer nếu còn tồn tại
        redisTemplate.delete("trip:dispatch:queue:" + tripId);
        redisTemplate.delete("trip:dispatch:current:" + tripId);

        trip.setStatus(TripStatus.Cancelled);
        trip.setCancelReason(cancelReason != null ? cancelReason : "Khách hàng hủy chuyến");
        trip.setCancelledBy("CUSTOMER");
        trip.setCancelledAt(LocalDateTime.now());

        Trip updatedTrip = tripRepository.save(trip);
        log.info("[HỦY CHUYẾN KHÁCH] Trip #{} đã bị hủy bởi Khách #{}", tripId, customerId);

        // Bắn Event sang Kafka để Payment Service hoàn lại tiền tạm giữ (nếu là WALLET)
        publishTripStatusEvent(updatedTrip, TripStatus.Cancelled);

        return mapToTripResponse(updatedTrip);
    }

    @Override
    public TripResponse acceptTrip(Long tripId) {
        // 1. Lấy Driver ID hiện tại từ Security Context (Token)
        Long driverId = SecurityUtils.getCurrentUserId(); // Hoặc Custom Header User ID của bạn

        // 2. LỚP BẢO VỆ 1: Kiểm tra trạng thái Chuyến đi trong Database
        Trip trip = tripRepository.findById(tripId)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy chuyến đi #" + tripId));

        if (trip.getStatus() != TripStatus.Requested) {
            throw new TripException("Rất tiếc! Chuyến đi đã bị hủy hoặc đã có tài xế khác nhận.");
        }

        // 3. LỚP BẢO VỆ 2: Kiểm tra Offer trong Redis (Chống quá hạn 30s hoặc nhận nhầm chuyến)
        String currentOfferKey = "trip:dispatch:current:" + tripId;
        String currentAssignedDriver = redisTemplate.opsForValue().get(currentOfferKey);

        if (currentAssignedDriver == null) {
            throw new TripException("Thời gian chờ 30 giây đã hết! Chuyến đi đã chuyển sang tài xế khác.");
        }

        if (!currentAssignedDriver.equals(String.valueOf(driverId))) {
            throw new TripException("Bạn không có quyền nhận chuyến đi này!");
        }

        // 4. CHẤP NHẬN CHUYẾN ĐI THÀNH CÔNG
        trip.setDriverId(driverId);
        trip.setStatus(TripStatus.Accepted);
        trip.setAcceptedAt(LocalDateTime.now());
        Trip updatedTrip = tripRepository.save(trip);

        // 5. DỌN DẸP REDIS CACHE (Hủy hàng đợi dispatch vì đã có người nhận)
        redisTemplate.delete("trip:dispatch:queue:" + tripId);
        redisTemplate.delete(currentOfferKey);

        trip.setDriverId(driverId);
        trip.setStatus(TripStatus.Accepted);
        trip.setAcceptedAt(LocalDateTime.now());

        // 1. Xóa ngay khỏi danh sách tài xế rảnh bên Redis GEO
        String geoKey = "drivers:available:" + trip.getVehicleType();
        redisTemplate.opsForZSet().remove(geoKey, String.valueOf(driverId));

        // 2. Bắn Event sang Kafka để User/Location Service cập nhật status = BUSY
        publishDriverStatusEvent(driverId, DriverStatus.BUSY);

        log.info("[TRIP ACCEPTED] Tài xế #{} đã nhận Trip #{} (Trạng thái -> BUSY)", driverId, tripId);

        // 7. Bắn Event lên Kafka 'trip-status-topic' để thông báo cho Khách hàng via WebSocket
        publishTripStatusEvent(updatedTrip, TripStatus.Accepted);

        return mapToTripResponse(updatedTrip);
    }

    private void publishDriverStatusEvent(Long driverId, DriverStatus status) {
        DriverStatusEvent statusEvent = DriverStatusEvent.builder()
                .driverId(driverId)
                .status(status)
                .build();

        kafkaTemplate.send("driver-status-topic", String.valueOf(driverId), statusEvent);
        log.info("[KAFKA PUBLISH] Đã đồng bộ trạng thái Tài xế #{}: {}", driverId, status);
    }

    // Tài xế CẬP NHẬT TRẠNG THÁI TIẾN TRÌNH (ARRIVED -> IN_PROGRESS -> COMPLETED)
    @Override
    public TripResponse updateTripStatusByDriver(Long tripId) {
        Long driverId = SecurityUtils.getCurrentUserId();

        Trip trip = tripRepository.findByIdAndDriverId(tripId, driverId)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy chuyến đi #" + tripId + " do bạn đảm nhận!"));

        TripStatus currentStatus = trip.getStatus();

        if (currentStatus == TripStatus.Accepted) {
            // Chuyển sang: Đã đến điểm đón (ARRIVED)
            trip.setStatus(TripStatus.Arrived);
            trip.setArrivedAt(LocalDateTime.now());
            log.info("[TIẾN TRÌNH TRIPS] Trip #{}: Tài xế đã đến điểm đón (ARRIVED)", tripId);

        } else if (currentStatus == TripStatus.Arrived) {
            // Chuyển sang: Đang di chuyển chở khách (IN_PROGRESS)
            trip.setStatus(TripStatus.InProgress);
            trip.setStartedAt(LocalDateTime.now());
            log.info("[TIẾN TRÌNH TRIPS] Trip #{}: Bắt đầu chuyến đi (IN_PROGRESS)", tripId);

        } else if (currentStatus == TripStatus.InProgress) {
            // Chuyển sang: HOÀN THÀNH CHUYẾN ĐI (COMPLETED)
            trip.setStatus(TripStatus.Completed);
            trip.setCompletedAt(LocalDateTime.now());
            trip.setPaymentStatus("PAID"); // Đã thanh toán xong
            log.info("[HOÀN THÀNH CHUYẾN] Trip #{}: Đã hoàn thành (COMPLETED)", tripId);

            // Đổi trạng thái về ONLINE rảnh có thể tiếp tục chuyến tiếp theo
            publishDriverStatusEvent(driverId, DriverStatus.ONLINE);

            // Bắn Event chốt sổ doanh thu và trừ hoa hồng sàn/cộng tiền tài xế
            publishTripStatusEvent(trip, TripStatus.Completed);

        } else {
            throw new TripException("Trạng thái chuyến đi hiện tại (" + currentStatus + ") không thể cập nhật tiếp!");
        }

        Trip updatedTrip = tripRepository.save(trip);
        return mapToTripResponse(updatedTrip);
    }

    @Override
    public TripResponse cancelTripByDriver(Long tripId, String cancelReason) {
        Long driverId = SecurityUtils.getCurrentUserId();

        Trip trip = tripRepository.findByIdAndDriverId(tripId, driverId)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy chuyến đi #" + tripId + " do bạn đảm nhận!"));

        if (trip.getStatus() == TripStatus.Completed || trip.getStatus() == TripStatus.Cancelled) {
            throw new TripException("Chuyến đi đã hoàn thành hoặc đã bị hủy trước đó!");
        }

        trip.setStatus(TripStatus.Cancelled);
        trip.setCancelReason(cancelReason != null ? cancelReason : "Tài xế hủy chuyến");
        trip.setCancelledBy("DRIVER");
        trip.setCancelledAt(LocalDateTime.now());

        Trip updatedTrip = tripRepository.save(trip);

        publishDriverStatusEvent(driverId, DriverStatus.ONLINE);

        log.info("[HỦY CHUYẾN TÀI XẾ] Trip #{} đã bị hủy bởi Tài xế #{}", tripId, driverId);

        // Bắn Event sang Kafka để Payment Service refund tiền giữ của Khách
        publishTripStatusEvent(updatedTrip, TripStatus.Cancelled);

        return mapToTripResponse(updatedTrip);
    }

    // Xem lịch sử chuyến đi của Tài xế
    @Override
    public List<TripResponse> getDriverTripHistory() {
        Long driverId = SecurityUtils.getCurrentUserId();
        List<Trip> trips = tripRepository.findByDriverIdOrderByCreatedAtDesc(driverId);
        return trips.stream().map(this::mapToTripResponse).collect(Collectors.toList());
    }

    private void publishTripStatusEvent(Trip trip, TripStatus tripStatus) {
        TripStatusEvent statusEvent = TripStatusEvent.builder()
                .tripId(trip.getId())
                .customerId(trip.getCustomerId())
                .driverId(trip.getDriverId())
                .fareAmount(trip.getFareAmount())
                .paymentMethod(trip.getPaymentMethod())
                .tripStatus(tripStatus)
                .cancelReason(trip.getCancelReason())
                .cancelledBy(trip.getCancelledBy())
                .build();

        // Bắn lên topic chung 'trip-status-topic'
        kafkaTemplate.send("trip-status-topic", String.valueOf(trip.getId()), statusEvent);
        log.info("[KAFKA PUBLISH] Đã bắn TripStatusEvent ({}) cho Trip #{}", tripStatus, trip.getId());
    }

    private TripResponse mapToTripResponse(Trip trip) {
        return TripResponse.builder()
                .tripId(trip.getId())
                .customerId(trip.getCustomerId())
                .driverId(trip.getDriverId())
                .pickupAddress(trip.getPickupAddress())
                .dropoffAddress(trip.getDropoffAddress())
                .vehicleType(trip.getVehicleType())
                .distanceKm(trip.getDistanceKm())
                .durationMinutes(trip.getDurationMinutes())
                .fareAmount(trip.getFareAmount())
                .paymentMethod(trip.getPaymentMethod())
                .paymentStatus(trip.getPaymentStatus())
                .tripStatus(trip.getStatus())
                .cancelReason(trip.getCancelReason())
                .cancelledBy(trip.getCancelledBy())
                .createdAt(trip.getCreatedAt())
                .updatedAt(trip.getUpdatedAt())
                .build();
    }
}
