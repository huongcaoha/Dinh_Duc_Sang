// ==========================================
// TOÀN CỤC: STATE QUẢN LÝ TÀI XẾ & KHÁCH HÀNG
// ==========================================
let map;
let activeDriverMarker;
let onlineDriverMarkers = [];

// Marker & Route cho giả lập đặt xe
let pickupMarker = null;
let dropoffMarker = null;
let routeLine = null;

// Tọa độ & Trạng thái Tài xế
let currentLat = 21.0378;
let currentLng = 105.7839;
let lastSentLat = null;
let lastSentLng = null;
let currentDriverStatus = "OFFLINE"; // OFFLINE, ONLINE, BUSY
let currentDriverId = 1;
let currentVehicleType = "BIKE";

// WebSocket STOMP
let stompClient = null;
let wsConnected = false;
let gpsStreamInterval = null;

// Chuyến đi hiện tại (Active Trip)
let activeTrip = null;
let offerCountdownInterval = null;
let movementSimulationInterval = null;

// Tọa độ mẫu cho Khách Hàng đặt xe
let customerPickupCoords = { lat: 21.0173, lng: 105.7838 }; // Keangnam
let customerDropoffCoords = { lat: 21.0287, lng: 105.8524 }; // Hồ Gươm
let customerVehicleType = "BIKE";
let customerBaseRate = 14000;
let customerPerKmRate = 5000;

// ==========================================
// KHỞI TẠO BẢN ĐỒ LEAFLET & WEBSOCKET
// ==========================================
window.onload = function () {
  initMap();
  connectWebSocket();
  startGpsStreamLoop();
  updateEstimateFare();
  loadAllMapData();
};

function initMap() {
  map = L.map("map").setView([currentLat, currentLng], 13);

  const LOCATIONIQ_TOKEN = "pk.7939da0d43354ae446e39ac07086b84f";
  L.tileLayer(
    `https://{s}-tiles.locationiq.com/v3/streets/r/{z}/{x}/{y}.png?key=${LOCATIONIQ_TOKEN}`,
    { maxZoom: 19, attribution: "© LocationIQ / OpenStreetMap" },
  ).addTo(map);

  setTimeout(() => map.invalidateSize(), 300);

  // Marker đại diện cho tài xế của chúng ta
  const driverIcon = L.icon({
    iconUrl: "https://cdn-icons-png.flaticon.com/512/3082/3082383.png",
    iconSize: [40, 40],
    iconAnchor: [20, 40],
    popupAnchor: [0, -40],
  });

  activeDriverMarker = L.marker([currentLat, currentLng], {
    icon: driverIcon,
    draggable: true,
  })
    .addTo(map)
    .bindPopup("<b>🎯 Xe của bạn (#" + currentDriverId + ")</b><br>Kéo thả để đổi tọa độ")
    .openPopup();

  activeDriverMarker.on("dragend", function () {
    const pos = activeDriverMarker.getLatLng();
    updateDriverCoords(pos.lat, pos.lng);
    addLog(`📍 [MAP DRAG] Bạn đã dời xe tới: (${currentLat.toFixed(5)}, ${currentLng.toFixed(5)})`);
  });

  map.on("click", function (e) {
    const activeTab = document.querySelector(".tab-pane.active")?.id;
    if (activeTab === "tabDriverContent") {
      activeDriverMarker.setLatLng(e.latlng);
      updateDriverCoords(e.latlng.lat, e.latlng.lng);
      addLog(`📍 [MAP CLICK] Đã chuyển vị trí xe tới: (${currentLat.toFixed(5)}, ${currentLng.toFixed(5)})`);
    } else {
      // Đang ở Tab Khách hàng: Click để chọn điểm đón hoặc điểm trả
      handleCustomerMapClick(e.latlng);
    }
  });

  updateDriverCoords(currentLat, currentLng);
}

function updateDriverCoords(lat, lng) {
  currentLat = lat;
  currentLng = lng;
  const input = document.getElementById("selectedCoords");
  if (input) input.value = `${lat.toFixed(5)}, ${lng.toFixed(5)}`;
}

// ==========================================
// 1. KẾT NỐI WEBSOCKET QUA STOMP / SOCKJS
// ==========================================
function connectWebSocket() {
  const wsDot = document.getElementById("wsDot");
  const wsText = document.getElementById("wsStatusText");

  if (wsDot) {
    wsDot.className = "ws-dot";
  }
  if (wsText) wsText.innerText = "WebSocket: Đang kết nối...";

  try {
    // Ưu tiên kết nối trực tiếp cổng 8086 của websocket-gateway-service
    const wsUrl = "http://localhost:8086/ws";
    addLog(`🔌 Đang kết nối WebSocket tới: ${wsUrl}...`);

    const socket = new SockJS(wsUrl);
    stompClient = Stomp.over(socket);
    stompClient.debug = null; // Tắt log debug rác của STOMP trên console

    stompClient.connect(
      {},
      function (frame) {
        wsConnected = true;
        if (wsDot) wsDot.className = "ws-dot connected";
        if (wsText) wsText.innerText = "WebSocket: Đã kết nối (Cổng 8086)";
        addLog("✅ [WEBSOCKET CONNECTED] Kết nối Realtime thành công!");

        // 1. Đăng ký nhận thông báo nổ cuốc (30s countdown) từ Kafka qua WebSocket
        const driverOfferDest = `/topic/driver/${currentDriverId}/ride-request`;
        stompClient.subscribe(driverOfferDest, function (message) {
          try {
            const offerData = JSON.parse(message.body);
            addLog(`🔔 [KAFKA WS] Nhận sự kiện nổ chuyến mới #${offerData.tripId || offerData.id}!`);
            showRideOfferModal(offerData);
          } catch (e) {
            console.error("Lỗi parse ride offer", e);
          }
        });

        // 2. Đăng ký nhận Offer từ Redis PubSub (kênh dự phòng)
        stompClient.subscribe(`/topic/driver/${currentDriverId}/offer`, function (message) {
          try {
            const offerData = JSON.parse(message.body);
            addLog(`🔔 [REDIS WS] Nhận Offer chuyến mới #${offerData.tripId}!`);
            showRideOfferModal(offerData);
          } catch (e) {
            console.error("Lỗi parse offer", e);
          }
        });
      },
      function (error) {
        wsConnected = false;
        if (wsDot) wsDot.className = "ws-dot disconnected";
        if (wsText) wsText.innerText = "WebSocket: Mất kết nối (Thử lại sau 4s)";
        addLog(`⚠️ [WS ERROR] Mất kết nối WebSocket. Sẽ tự động kết nối lại...`);
        setTimeout(connectWebSocket, 4000);
      }
    );
  } catch (err) {
    console.error("Lỗi khởi tạo WebSocket", err);
    setTimeout(connectWebSocket, 4000);
  }
}

// ==========================================
// 2. VÒNG LẶP GỬI GPS QUA WEBSOCKET 1S / LẦN
// (CÓ KIỂM TRA TỌA ĐỘ THAY ĐỔI TRÁNH SPAM)
// ==========================================
function startGpsStreamLoop() {
  if (gpsStreamInterval) clearInterval(gpsStreamInterval);

  gpsStreamInterval = setInterval(() => {
    checkAndSendGpsStream();
  }, 1000); // 1 giây / lần đúng theo yêu cầu
}

function checkAndSendGpsStream() {
  // Chỉ gửi khi tài xế đang ONLINE hoặc BUSY
  if (currentDriverStatus === "OFFLINE") return;

  const streamToggle = document.getElementById("wsGpsStreamToggle");
  if (streamToggle && !streamToggle.checked) return;

  // KIỂM TRA VỊ TRÍ TRƯỚC ĐÓ: Nếu không di chuyển thì KHÔNG gửi cập nhật lại
  const threshold = 0.00001; // ~1.1 mét
  const deltaLat = lastSentLat === null ? 999 : Math.abs(currentLat - lastSentLat);
  const deltaLng = lastSentLng === null ? 999 : Math.abs(currentLng - lastSentLng);

  const hasMoved = deltaLat > threshold || deltaLng > threshold;

  if (!hasMoved) {
    // Tọa độ không thay đổi hoặc tài xế đứng yên -> KHÔNG CẦN CẬP NHẬT LẠI BACKEND
    return;
  }

  // Cập nhật vị trí đã gửi
  lastSentLat = currentLat;
  lastSentLng = currentLng;

  const payload = {
    driverId: currentDriverId,
    tripId: activeTrip ? activeTrip.id : null,
    lat: parseFloat(currentLat.toFixed(6)),
    lng: parseFloat(currentLng.toFixed(6)),
    vehicleType: currentVehicleType,
  };

  // 1. Gửi qua STOMP WebSocket lên endpoint /app/location/stream
  if (stompClient && stompClient.connected) {
    stompClient.send("/app/location/stream", {}, JSON.stringify(payload));
    addLog(`📡 [WS GPS 1s] Đã gửi vị trí mới: (${payload.lat}, ${payload.lng}) | Xe: ${payload.vehicleType}`);
  } else {
    // Dự phòng nếu WebSocket chưa bật: Gửi qua REST API
    sendDriverLocationRestSilently(payload);
  }
}

async function sendDriverLocationRestSilently(payload) {
  try {
    await fetch("http://localhost:8080/location-tracking-service/api/v1/location/driver", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        driverId: payload.driverId,
        latitude: payload.lat,
        longitude: payload.lng,
        vehicleType: payload.vehicleType,
      }),
    });
  } catch (e) {
    // Im lặng bỏ qua khi stream
  }
}

function onToggleGpsStream(enabled) {
  if (enabled) {
    addLog("🟢 Đã BẬT chế độ tự động Stream GPS qua WebSocket (1s/lần).");
    lastSentLat = null; // Kích hoạt gửi ngay tọa độ hiện tại
  } else {
    addLog("🔴 Đã TẮT chế độ tự động Stream GPS qua WebSocket.");
  }
}

function onChangeVehicleType(type) {
  currentVehicleType = type;
  addLog(`🚗 Đã chọn loại xe hoạt động: ${type}`);
  lastSentLat = null; // Ép gửi cập nhật loại xe mới lên Redis
  checkAndSendGpsStream();
}

function simulateRandomStep() {
  // Nhích nhẹ vị trí để test tính năng phát hiện di chuyển
  const stepLat = (Math.random() - 0.5) * 0.0006;
  const stepLng = (Math.random() - 0.5) * 0.0006;
  currentLat += stepLat;
  currentLng += stepLng;
  activeDriverMarker.setLatLng([currentLat, currentLng]);
  updateDriverCoords(currentLat, currentLng);
  addLog(`👟 [TEST DI CHUYỂN] Bạn vừa nhích xe: (${currentLat.toFixed(5)}, ${currentLng.toFixed(5)})`);
}

// ==========================================
// 3. QUẢN LÝ TAB (TÀI XẾ vs ĐẶT XE KHÁCH)
// ==========================================
function switchTab(tab) {
  const btnDriver = document.getElementById("tabBtnDriver");
  const btnCustomer = document.getElementById("tabBtnCustomer");
  const paneDriver = document.getElementById("tabDriverContent");
  const paneCustomer = document.getElementById("tabCustomerContent");

  if (tab === "driver") {
    btnDriver.classList.add("active");
    btnCustomer.classList.remove("active");
    paneDriver.classList.add("active");
    paneCustomer.classList.remove("active");
  } else {
    btnCustomer.classList.add("active");
    btnDriver.classList.remove("active");
    paneCustomer.classList.add("active");
    paneDriver.classList.remove("active");
    updateCustomerMapMarkers();
  }
}

// ==========================================
// 4. GIẢ LẬP ĐẶT XE PHÍA KHÁCH HÀNG (CUSTOMER)
// ==========================================
function selectCustomerVehicle(type, base, perKm) {
  customerVehicleType = type;
  customerBaseRate = base;
  customerPerKmRate = perKm;

  document.querySelectorAll(".vehicle-card").forEach((c) => c.classList.remove("active"));
  if (type === "BIKE") document.getElementById("cardBike")?.classList.add("active");
  if (type === "CAR_4_SEAT") document.getElementById("cardCar4")?.classList.add("active");
  if (type === "CAR_7_SEAT") document.getElementById("cardCar7")?.classList.add("active");

  updateEstimateFare();
}

function setPickupPreset(preset) {
  const presets = {
    keangnam: { name: "Keangnam Landmark 72, Nam Từ Liêm, Hà Nội", lat: 21.0173, lng: 105.7838 },
    mydinh: { name: "Bến xe Mỹ Đình, Nam Từ Liêm, Hà Nội", lat: 21.0285, lng: 105.7778 },
    dhqg: { name: "ĐH Quốc Gia Hà Nội, 144 Xuân Thủy, Cầu Giấy", lat: 21.0371, lng: 105.7818 },
  };
  const p = presets[preset];
  if (!p) return;

  customerPickupCoords = { lat: p.lat, lng: p.lng };
  document.getElementById("custPickupAddress").value = p.name;
  document.getElementById("custPickupCoordsTag").innerText = `📍 ${p.lat.toFixed(4)}, ${p.lng.toFixed(4)}`;
  updateCustomerMapMarkers();
  updateEstimateFare();
}

function setDropoffPreset(preset) {
  const presets = {
    hoguom: { name: "Hồ Hoàn Kiếm, Tràng Tiền, Hoàn Kiếm, Hà Nội", lat: 21.0287, lng: 105.8524 },
    vincom: { name: "Vincom Center Bà Triệu, Hai Bà Trưng, Hà Nội", lat: 21.0118, lng: 105.8504 },
    noibai: { name: "Sân bay Quốc tế Nội Bài, Sóc Sơn, Hà Nội", lat: 21.2187, lng: 105.8042 },
  };
  const p = presets[preset];
  if (!p) return;

  customerDropoffCoords = { lat: p.lat, lng: p.lng };
  document.getElementById("custDropoffAddress").value = p.name;
  document.getElementById("custDropoffCoordsTag").innerText = `📍 ${p.lat.toFixed(4)}, ${p.lng.toFixed(4)}`;
  updateCustomerMapMarkers();
  updateEstimateFare();
}

function handleCustomerMapClick(latlng) {
  // Click map ở tab Khách: cập nhật điểm đón
  customerPickupCoords = { lat: latlng.lat, lng: latlng.lng };
  document.getElementById("custPickupAddress").value = `Vị trí chọn trên Map (${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)})`;
  document.getElementById("custPickupCoordsTag").innerText = `📍 ${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`;
  updateCustomerMapMarkers();
  updateEstimateFare();
  addLog(`📍 [KHÁCH HÀNG] Đã chọn Điểm đón: (${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)})`);
}

function updateCustomerMapMarkers() {
  // Điểm đón (Xanh)
  if (pickupMarker) map.removeLayer(pickupMarker);
  const pickupIcon = L.icon({
    iconUrl: "https://cdn-icons-png.flaticon.com/512/14090/14090313.png",
    iconSize: [36, 36],
    iconAnchor: [18, 36],
  });
  pickupMarker = L.marker([customerPickupCoords.lat, customerPickupCoords.lng], {
    icon: pickupIcon,
  })
    .addTo(map)
    .bindPopup("<b>🟢 Điểm đón khách</b><br>" + document.getElementById("custPickupAddress").value);

  // Điểm trả (Đỏ)
  if (dropoffMarker) map.removeLayer(dropoffMarker);
  const dropoffIcon = L.icon({
    iconUrl: "https://cdn-icons-png.flaticon.com/512/9131/9131546.png",
    iconSize: [36, 36],
    iconAnchor: [18, 36],
  });
  dropoffMarker = L.marker([customerDropoffCoords.lat, customerDropoffCoords.lng], {
    icon: dropoffIcon,
  })
    .addTo(map)
    .bindPopup("<b>🔴 Điểm trả khách</b><br>" + document.getElementById("custDropoffAddress").value);

  // Đường kẻ lộ trình
  drawRouteBetween([customerPickupCoords.lat, customerPickupCoords.lng], [customerDropoffCoords.lat, customerDropoffCoords.lng]);
}

function drawRouteBetween(pointA, pointB) {
  if (routeLine) map.removeLayer(routeLine);
  routeLine = L.polyline([pointA, pointB], {
    color: "#f59e0b",
    weight: 4,
    opacity: 0.8,
    dashArray: "6, 8",
  }).addTo(map);
}

function updateEstimateFare() {
  const dist = getDistanceFromLatLonInKm(
    customerPickupCoords.lat,
    customerPickupCoords.lng,
    customerDropoffCoords.lat,
    customerDropoffCoords.lng
  );

  let fare = customerBaseRate;
  if (dist > 2.0) {
    fare += (dist - 2.0) * customerPerKmRate;
  }
  fare = Math.round(fare / 1000) * 1000;

  const distText = document.getElementById("estDistanceText");
  const fareText = document.getElementById("estFareText");
  if (distText) distText.innerText = `~ ${dist.toFixed(1)} km`;
  if (fareText) fareText.innerText = `${fare.toLocaleString("vi-VN")} VNĐ`;
}

// BẤM NÚT ĐẶT XE
async function createCustomerTrip() {
  const pickupAddr = document.getElementById("custPickupAddress").value;
  const dropoffAddr = document.getElementById("custDropoffAddress").value;
  const payMethod = document.getElementById("custPaymentMethod").value;

  const dist = getDistanceFromLatLonInKm(
    customerPickupCoords.lat,
    customerPickupCoords.lng,
    customerDropoffCoords.lat,
    customerDropoffCoords.lng
  );

  let fare = customerBaseRate;
  if (dist > 2.0) fare += (dist - 2.0) * customerPerKmRate;
  fare = Math.round(fare / 1000) * 1000;

  addLog(`🚖 [ĐẶT XE] Đang gửi yêu cầu đặt xe [${customerVehicleType}] từ "${pickupAddr}" đến "${dropoffAddr}"...`);

  // Tạo đối tượng chuyến đi
  const newTrip = {
    id: Math.floor(Math.random() * 9000 + 1000),
    customerId: 99,
    pickupAddress: pickupAddr,
    dropoffAddress: dropoffAddr,
    pickupLat: customerPickupCoords.lat,
    pickupLng: customerPickupCoords.lng,
    dropoffLat: customerDropoffCoords.lat,
    dropoffLng: customerDropoffCoords.lng,
    vehicleType: customerVehicleType,
    paymentMethod: payMethod,
    distanceKm: parseFloat(dist.toFixed(1)),
    fareAmount: fare,
    status: "Requested",
    candidateDriverIds: [String(currentDriverId)],
  };

  // Cố gắng gọi API backend nếu Trip Service đang chạy
  try {
    const res = await fetch("http://localhost:8080/trip-dispatch-service/api/v1/trips", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        pickupAddress: pickupAddr,
        dropoffAddress: dropoffAddr,
        vehicleType: customerVehicleType,
        paymentMethod: payMethod,
      }),
    });
    if (res.ok) {
      const data = await res.json();
      if (data?.data?.id) newTrip.id = data.data.id;
    }
  } catch (e) {
    // Nếu backend chưa bật đầy đủ, tiếp tục flow giả lập mô phỏng
  }

  // Cập nhật giao diện Khách hàng: Hiển thị Hộp Theo dõi đơn hàng
  document.getElementById("custActiveTripBox").style.display = "flex";
  document.getElementById("custTripIdBadge").innerText = `Trip #${newTrip.id}`;
  document.getElementById("custTripStatusBadge").innerText = "TÌM XE...";
  document.getElementById("custTripStatusMessage").innerHTML = `
    <i class="fa-solid fa-spinner fa-spin"></i>
    <span>Đang gửi thông báo đề nghị chuyến đi tới Tài xế #${currentDriverId} (Hạn 30s)...</span>
  `;

  addLog(`📢 [DISPATCH] Hệ thống tự động gán đơn #${newTrip.id} cho Tài xế #${currentDriverId}!`);

  // Gửi thông báo nổ cuốc sang Tab Tài xế (Đồng hồ đếm ngược 30s)
  setTimeout(() => {
    showRideOfferModal(newTrip);
  }, 500);
}

// ==========================================
// 5. MODAL NỔ CUỐC & ĐẾM NGƯỢC 30S (TÀI XẾ)
// ==========================================
let currentOfferData = null;

function showRideOfferModal(offerData) {
  currentOfferData = offerData;
  const modal = document.getElementById("rideOfferModal");
  if (!modal) return;

  const tripId = offerData.tripId || offerData.id || "101";
  document.getElementById("offerTripTitle").innerText = `Chuyến đi #${tripId}`;
  document.getElementById("offerPickupAddress").innerText = offerData.pickupAddress || "Điểm đón khách";
  document.getElementById("offerDropoffAddress").innerText = offerData.dropoffAddress || "Điểm trả khách";
  document.getElementById("offerFare").innerText = (offerData.amount || offerData.fareAmount || 41500).toLocaleString("vi-VN") + " đ";
  document.getElementById("offerDistance").innerText = (offerData.distanceKm || 5.2) + " km";
  document.getElementById("offerVehicle").innerText = offerData.vehicleType || currentVehicleType;

  modal.style.display = "flex";

  // Đếm ngược 30 giây
  let secondsLeft = 30;
  const timerBadge = document.getElementById("offerTimer");
  timerBadge.innerText = `${secondsLeft}s`;

  if (offerCountdownInterval) clearInterval(offerCountdownInterval);
  offerCountdownInterval = setInterval(() => {
    secondsLeft--;
    timerBadge.innerText = `${secondsLeft}s`;

    if (secondsLeft <= 0) {
      clearInterval(offerCountdownInterval);
      rejectCurrentOffer(true); // Hết giờ -> Tự động từ chối
    }
  }, 1000);
}

// TÀI XẾ TỪ CHỐI ĐƠN
function rejectCurrentOffer(isTimeout = false) {
  if (offerCountdownInterval) clearInterval(offerCountdownInterval);
  document.getElementById("rideOfferModal").style.display = "none";

  const tripId = currentOfferData?.tripId || currentOfferData?.id;
  if (isTimeout) {
    addLog(`⏰ [HẾT GIỜ] Tài xế #${currentDriverId} không phản hồi đơn #${tripId} sau 30s!`);
  } else {
    addLog(`❌ [TỪ CHỐI] Tài xế #${currentDriverId} đã BẤM TỪ CHỐI chuyến đi #${tripId}!`);
  }

  // Cập nhật lại giao diện Khách
  const custMsg = document.getElementById("custTripStatusMessage");
  if (custMsg) {
    custMsg.innerHTML = `<i class="fa-solid fa-triangle-exclamation" style="color:#ef4444"></i> <span style="color:#ef4444">Tài xế #${currentDriverId} đã từ chối nhận đơn. Vui lòng thử lại!</span>`;
  }
  const custStatus = document.getElementById("custTripStatusBadge");
  if (custStatus) {
    custStatus.innerText = "TỪ CHỐI";
    custStatus.style.background = "#ef4444";
  }

  currentOfferData = null;
}

// TÀI XẾ NHẬN CHUYẾN
async function acceptCurrentOffer() {
  if (offerCountdownInterval) clearInterval(offerCountdownInterval);
  document.getElementById("rideOfferModal").style.display = "none";

  activeTrip = currentOfferData;
  const tripId = activeTrip.tripId || activeTrip.id;

  addLog(`🎉 [NHẬN CHUYẾN] Tài xế #${currentDriverId} đã NHẬN đơn #${tripId}!`);

  // Chuyển trạng thái Tài xế sang BUSY
  updateStatusUI("BUSY");

  // Cố gắng gọi API backend nếu có
  try {
    await fetch(`http://localhost:8080/trip-dispatch-service/api/v1/trips/${tripId}/accept`, {
      method: "PUT",
    });
  } catch (e) {}

  // 1. Cập nhật giao diện bên Tab Tài Xế
  document.getElementById("driverActiveTripBox").style.display = "flex";
  document.getElementById("driverTripIdBadge").innerText = `Trip #${tripId}`;
  document.getElementById("driverTripStatusBadge").innerText = "ACCEPTED";
  document.getElementById("driverTripPickupText").innerText = activeTrip.pickupAddress || "Điểm đón";
  document.getElementById("driverTripDropoffText").innerText = activeTrip.dropoffAddress || "Điểm trả";
  document.getElementById("driverTripFareText").innerText = (activeTrip.amount || activeTrip.fareAmount || 41500).toLocaleString("vi-VN") + " VNĐ";
  document.getElementById("driverSimAlertText").innerText = "Đang tự động di chuyển đến điểm đón (GPS 1s/lần)...";
  document.getElementById("btnDriverNextStatus").innerHTML = `<i class="fa-solid fa-flag-checkered"></i> ĐẾN ĐIỂM ĐÓN (ARRIVED)`;

  // 2. Cập nhật giao diện bên Tab Khách Hàng
  document.getElementById("custTripStatusBadge").innerText = "TÀI XẾ ĐANG TỚI";
  document.getElementById("custTripStatusBadge").style.background = "#22c55e";
  document.getElementById("custTripStatusMessage").innerHTML = `
    <i class="fa-solid fa-car-side fa-beat"></i>
    <span>Tài xế #${currentDriverId} đã nhận chuyến! Đang di chuyển tới điểm đón bạn...</span>
  `;
  document.getElementById("custAssignedDriverText").innerText = `Tài xế #${currentDriverId} (${currentVehicleType})`;

  // Vẽ lộ trình từ Tài xế -> Điểm đón của Khách
  const pickupLat = activeTrip.pickupLat || customerPickupCoords.lat;
  const pickupLng = activeTrip.pickupLng || customerPickupCoords.lng;
  drawRouteBetween([currentLat, currentLng], [pickupLat, pickupLng]);

  // Bắt đầu mô phỏng tài xế tự động di chuyển tới điểm đón (GPS stream 1s/lần)
  startDriverMovementSimulation(pickupLat, pickupLng, onArrivedAtPickup);
}

// ==========================================
// 6. GIẢ LẬP TÀI XẾ DI CHUYỂN TỚI ĐIỂM ĐÓN
// (MỖI 1S NHÍCH 1 ĐOẠN -> TỰ ĐỘNG STREAM WEBSOCKET)
// ==========================================
function startDriverMovementSimulation(targetLat, targetLng, onCompleteCallback) {
  if (movementSimulationInterval) clearInterval(movementSimulationInterval);

  addLog(`🚗 [GIẢ LẬP DI CHUYỂN] Bắt đầu điều khiển xe chạy về đích (${targetLat.toFixed(4)}, ${targetLng.toFixed(4)})...`);

  movementSimulationInterval = setInterval(() => {
    const distKm = getDistanceFromLatLonInKm(currentLat, currentLng, targetLat, targetLng);
    const distMeters = Math.round(distKm * 1000);

    // Cập nhật khoảng cách trên UI cả 2 tab
    const driverDist = document.getElementById("driverDistanceToPickupText");
    const custDist = document.getElementById("custDriverDistanceText");
    if (driverDist) driverDist.innerText = `${distMeters} m`;
    if (custDist) custDist.innerText = `${distMeters} m`;

    // Nếu đã tới gần đích (< 35m)
    if (distMeters <= 35) {
      clearInterval(movementSimulationInterval);
      currentLat = targetLat;
      currentLng = targetLng;
      activeDriverMarker.setLatLng([currentLat, currentLng]);
      updateDriverCoords(currentLat, currentLng);

      if (driverDist) driverDist.innerText = `ĐÃ ĐẾN NƠI`;
      if (custDist) custDist.innerText = `ĐÃ ĐẾN NƠI`;

      if (onCompleteCallback) onCompleteCallback();
      return;
    }

    // Nhích dần về phía đích (Mỗi 1s di chuyển ~0.0003 độ ≈ 35m)
    const step = 0.00035;
    const ratio = Math.min(step / (distKm || 0.001), 0.25);

    currentLat += (targetLat - currentLat) * ratio;
    currentLng += (targetLng - currentLng) * ratio;

    // Cập nhật marker trên bản đồ
    activeDriverMarker.setLatLng([currentLat, currentLng]);
    updateDriverCoords(currentLat, currentLng);

    // VÒNG LẶP GPS 1S Ở TRÊN SẼ TỰ ĐỘNG PHÁT HIỆN TỌA ĐỘ THAY ĐỔI
    // VÀ BẮN WEBSOCKET TỚI BACKEND ĐỂ CẬP NHẬT GEO REDIS!
  }, 1000);
}

// KHI TÀI XẾ ĐÃ ĐẾN ĐIỂM ĐÓN (ARRIVED)
function onArrivedAtPickup() {
  addLog("🎯 [ARRIVED] Tài xế đã tới điểm đón khách hàng!");

  // Cập nhật Tab Tài xế
  document.getElementById("driverTripStatusBadge").innerText = "ARRIVED";
  document.getElementById("driverSimAlertText").innerText = "Đã đến điểm đón! Đang chờ khách lên xe...";
  document.getElementById("btnDriverNextStatus").innerHTML = `<i class="fa-solid fa-play"></i> BẮT ĐẦU CHUYẾN (IN_PROGRESS)`;

  // Cập nhật Tab Khách hàng
  document.getElementById("custTripStatusBadge").innerText = "TÀI XẾ ĐÃ ĐẾN";
  document.getElementById("custTripStatusMessage").innerHTML = `
    <i class="fa-solid fa-person-walking" style="color:#22c55e"></i>
    <span>Tài xế đã có mặt tại điểm đón! Vui lòng ra xe.</span>
  `;
}

// TÀI XẾ BẤM NÚT CẬP NHẬT TIẾN TRÌNH (ARRIVED -> IN_PROGRESS -> COMPLETED)
function advanceTripStatusByDriver() {
  if (!activeTrip) return;

  const currentBadge = document.getElementById("driverTripStatusBadge").innerText;

  if (currentBadge === "ACCEPTED") {
    // Ép chuyển luôn sang ARRIVED (bỏ qua đợi xe chạy xong)
    if (movementSimulationInterval) clearInterval(movementSimulationInterval);
    const dropLat = activeTrip.pickupLat || customerPickupCoords.lat;
    const dropLng = activeTrip.pickupLng || customerPickupCoords.lng;
    currentLat = dropLat;
    currentLng = dropLng;
    activeDriverMarker.setLatLng([currentLat, currentLng]);
    updateDriverCoords(currentLat, currentLng);
    onArrivedAtPickup();

  } else if (currentBadge === "ARRIVED") {
    // Bắt đầu chuyến đi (IN_PROGRESS)
    addLog("🚀 [IN_PROGRESS] Khách đã lên xe! Bắt đầu di chuyển đến điểm trả...");
    document.getElementById("driverTripStatusBadge").innerText = "IN_PROGRESS";
    document.getElementById("driverSimAlertText").innerText = "Đang chở khách đến điểm trả (GPS 1s/lần)...";
    document.getElementById("btnDriverNextStatus").innerHTML = `<i class="fa-solid fa-circle-check"></i> HOÀN THÀNH CHUYẾN (COMPLETED)`;

    document.getElementById("custTripStatusBadge").innerText = "ĐANG DI CHUYỂN";
    document.getElementById("custTripStatusMessage").innerHTML = `
      <i class="fa-solid fa-route fa-beat"></i>
      <span>Chuyến đi đang diễn ra! Bạn đang trên đường tới điểm trả.</span>
    `;

    // Vẽ đường đi tới điểm trả
    const dropoffLat = activeTrip.dropoffLat || customerDropoffCoords.lat;
    const dropoffLng = activeTrip.dropoffLng || customerDropoffCoords.lng;
    drawRouteBetween([currentLat, currentLng], [dropoffLat, dropoffLng]);

    // Giả lập xe chạy tới điểm trả khách
    startDriverMovementSimulation(dropoffLat, dropoffLng, onArrivedAtDropoff);

  } else if (currentBadge === "IN_PROGRESS") {
    // Hoàn thành chuyến đi
    onArrivedAtDropoff();
  }
}

// KHI TỚI ĐIỂM TRẢ -> HOÀN THÀNH CHUYẾN (COMPLETED)
function onArrivedAtDropoff() {
  if (movementSimulationInterval) clearInterval(movementSimulationInterval);

  addLog("🏁 [COMPLETED] Chuyến đi đã hoàn thành xuất sắc!");
  alert("🎉 Chúc mừng! Chuyến đi đã hoàn thành thành công.");

  // Dọn dẹp chuyến đi
  if (routeLine) map.removeLayer(routeLine);
  if (pickupMarker) map.removeLayer(pickupMarker);
  if (dropoffMarker) map.removeLayer(dropoffMarker);

  document.getElementById("driverActiveTripBox").style.display = "none";
  document.getElementById("custActiveTripBox").style.display = "none";

  // Giải phóng tài xế về ONLINE
  updateStatusUI("ONLINE");
  activeTrip = null;
  lastSentLat = null; // Kích hoạt gửi vị trí mới khi rảnh
  checkAndSendGpsStream();
}

// HỦY CHUYẾN
function cancelTripByDriver() {
  if (confirm("Bạn có chắc chắn muốn hủy chuyến đi này không?")) {
    if (movementSimulationInterval) clearInterval(movementSimulationInterval);
    addLog("🚫 Tài xế đã hủy chuyến đi!");
    document.getElementById("driverActiveTripBox").style.display = "none";
    document.getElementById("custTripStatusMessage").innerHTML = `
      <i class="fa-solid fa-ban" style="color:#ef4444"></i>
      <span style="color:#ef4444">Tài xế đã hủy chuyến. Vui lòng đặt lại!</span>
    `;
    updateStatusUI("ONLINE");
    activeTrip = null;
  }
}

function cancelTripByCustomer() {
  if (confirm("Bạn có chắc chắn muốn hủy chuyến xe đã đặt không?")) {
    if (movementSimulationInterval) clearInterval(movementSimulationInterval);
    addLog("🚫 Khách hàng đã hủy chuyến xe!");
    document.getElementById("custActiveTripBox").style.display = "none";
    document.getElementById("driverActiveTripBox").style.display = "none";
    updateStatusUI("ONLINE");
    activeTrip = null;
  }
}

// ==========================================
// 7. CÔNG TẮC NGUỒN ONLINE / OFFLINE (TÀI XẾ)
// ==========================================
async function onTogglePowerSwitch(checkbox) {
  if (currentDriverStatus === "BUSY") {
    alert("Bạn đang trong chuyến đi (BUSY), không thể tắt trạng thái hoạt động!");
    checkbox.checked = true;
    return;
  }

  const newStatus = checkbox.checked ? "ONLINE" : "OFFLINE";
  updateStatusUI(newStatus);

  if (newStatus === "ONLINE") {
    addLog("🟢 Tài xế đã BẬT ONLINE. Đang bắt đầu phát tọa độ...");
    lastSentLat = null;
    checkAndSendGpsStream();
  } else {
    addLog("🔴 Tài xế đã TẮT OFFLINE. Đã ngừng phát tọa độ.");
  }

  // Cố gắng đồng bộ với backend User Service nếu đang chạy
  try {
    await fetch("http://localhost:8080/user-service/api/v1/driver/status", {
      method: "PATCH",
    });
  } catch (e) {}
}

function updateStatusUI(status) {
  currentDriverStatus = status;
  const toggleInput = document.getElementById("driverStatusToggle");
  const statusText = document.getElementById("statusText");

  if (!toggleInput || !statusText) return;

  if (status === "ONLINE") {
    toggleInput.checked = true;
    toggleInput.disabled = false;
    statusText.innerText = "🟢 ONLINE";
    statusText.style.color = "#4ade80";
  } else if (status === "BUSY") {
    toggleInput.checked = true;
    toggleInput.disabled = true; // Khóa công tắc khi đang chở khách
    statusText.innerText = "🟡 BUSY (ĐANG CHỞ KHÁCH)";
    statusText.style.color = "#facc15";
  } else {
    toggleInput.checked = false;
    toggleInput.disabled = false;
    statusText.innerText = "🔴 OFFLINE";
    statusText.style.color = "#94a3b8";
  }
}

// CẬP NHẬT TỌA ĐỘ THỦ CÔNG (REST)
async function sendDriverLocation() {
  const payload = {
    latitude: parseFloat(currentLat.toFixed(6)),
    longitude: parseFloat(currentLng.toFixed(6)),
    vehicleType: currentVehicleType,
    driverId: currentDriverId,
  };

  addLog(`📡 [REST] Đang đẩy tọa độ (${payload.latitude}, ${payload.longitude})...`);

  try {
    const res = await fetch("http://localhost:8080/location-tracking-service/api/v1/location/driver", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (res.ok) {
      addLog("✅ [GPS REST SUCCESS] Đã lưu tọa độ vào hệ thống backend!");
      setTimeout(loadAllMapData, 500);
    } else {
      addLog(`❌ [LỖI GPS REST]: Status ${res.status}`);
    }
  } catch (err) {
    addLog(`❌ [LỖI KẾT NỐI] Không kết nối được location-tracking-service (Port 8080)!`);
  }
}

// TẢI TÀI XẾ XUNG QUANH LÊN MAP
async function loadAllMapData() {
  const radius = 20.0;
  const url = `http://localhost:8080/location-tracking-service/api/v1/location/nearby?lat=${currentLat}&lng=${currentLng}&radiusKm=${radius}&vehicleType=${currentVehicleType}`;

  try {
    const res = await fetch(url);
    if (!res.ok) return;
    const resData = await res.json();

    onlineDriverMarkers.forEach((m) => map.removeLayer(m));
    onlineDriverMarkers = [];

    const drivers = resData.data || resData;
    if (Array.isArray(drivers) && drivers.length > 0) {
      addLog(`🎉 Tìm thấy ${drivers.length} tài xế đang Online trên Map!`);

      const otherIcon = L.icon({
        iconUrl: "https://cdn-icons-png.flaticon.com/512/1048/1048314.png",
        iconSize: [30, 30],
        iconAnchor: [15, 30],
      });

      drivers.forEach((d) => {
        if (d.driverId == currentDriverId) return; // Không vẽ đè lên xe của mình
        const m = L.marker([d.latitude || d.lat, d.longitude || d.lng], {
          icon: otherIcon,
        })
          .addTo(map)
          .bindPopup(`<b>🚕 Xe khác #${d.driverId}</b><br>Khoảng cách: ${d.distanceKm || 0} km`);
        onlineDriverMarkers.push(m);
      });
    }
  } catch (err) {}
}

// ==========================================
// TIỆN ÍCH HỖ TRỢ
// ==========================================
function getDistanceFromLatLonInKm(lat1, lon1, lat2, lon2) {
  const R = 6371; // Bán kính Trái Đất (km)
  const dLat = deg2rad(lat2 - lat1);
  const dLon = deg2rad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function deg2rad(deg) {
  return deg * (Math.PI / 180);
}

function clearLogs() {
  const div = document.getElementById("logs");
  if (div) div.innerHTML = "";
}

function addLog(msg) {
  const div = document.getElementById("logs");
  if (!div) return;
  div.innerHTML += `[${new Date().toLocaleTimeString()}] ${msg}<br>`;
  div.scrollTop = div.scrollHeight;
}
