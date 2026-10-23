// ==========================================
// STATE TẬP TRUNG QUẢN LÝ TÀI XẾ
// ==========================================
let map;
let activeDriverMarker;
let onlineDriverMarkers = [];

let currentLat = 21.0378;
let currentLng = 105.7839;
let currentDriverStatus = "OFFLINE"; // Các trạng thái: OFFLINE, ONLINE, BUSY

// ==========================================
// KHỞI TẠO BẢN ĐỒ LEAFLET
// ==========================================
window.onload = function () {
  map = L.map("map").setView([currentLat, currentLng], 13);

  const LOCATIONIQ_TOKEN = "pk.7939da0d43354ae446e39ac07086b84f";
  L.tileLayer(
    `https://{s}-tiles.locationiq.com/v3/streets/r/{z}/{x}/{y}.png?key=${LOCATIONIQ_TOKEN}`,
    { maxZoom: 19, attribution: "© LocationIQ" },
  ).addTo(map);

  setTimeout(() => map.invalidateSize(), 200);

  // Marker đại diện cho điểm chọn tọa độ GPS
  const testIcon = L.icon({
    iconUrl: "https://cdn-icons-png.flaticon.com/512/3082/3082383.png",
    iconSize: [38, 38],
  });

  activeDriverMarker = L.marker([currentLat, currentLng], {
    icon: testIcon,
    draggable: true,
  })
    .addTo(map)
    .bindPopup("<b>🎯 Điểm chọn tọa độ GPS (Kéo/Click)</b>")
    .openPopup();

  activeDriverMarker.on("dragend", function () {
    const pos = activeDriverMarker.getLatLng();
    updateSelectedCoords(pos.lat, pos.lng);
  });

  map.on("click", function (e) {
    activeDriverMarker.setLatLng(e.latlng);
    updateSelectedCoords(e.latlng.lat, e.latlng.lng);
  });

  loadAllMapData();
};

function updateSelectedCoords(lat, lng) {
  currentLat = lat;
  currentLng = lng;
  const input = document.getElementById("selectedCoords");
  if (input) input.value = `${lat.toFixed(5)}, ${lng.toFixed(5)}`;
}

// ==========================================
// 1. TẢI DANH SÁCH TÀI XẾ ONLINE LÊN MAP
// ==========================================
function loadAllMapData() {
  addLog("🔄 Đang quét toàn bộ Tài xế Online...");
  fetchNearbyOnlineDrivers();
}

async function fetchNearbyOnlineDrivers() {
  const radius = 20.0;
  const vehicleType = document.getElementById("vehicleType")?.value || "BIKE";
  const url = `http://localhost:8080/location-tracking-service/api/v1/location/nearby?lat=${currentLat}&lng=${currentLng}&radiusKm=${radius}&vehicleType=${vehicleType}`;

  try {
    const res = await fetch(url);
    const resData = await res.json();

    // Xóa marker cũ
    onlineDriverMarkers.forEach((m) => map.removeLayer(m));
    onlineDriverMarkers = [];

    const drivers = resData.data || resData;
    if (Array.isArray(drivers) && drivers.length > 0) {
      addLog(`🎉 Tìm thấy ${drivers.length} tài xế đang Online trên Map!`);

      const driverOnlineIcon = L.icon({
        iconUrl: "https://cdn-icons-png.flaticon.com/512/1048/1048314.png",
        iconSize: [30, 30],
      });

      drivers.forEach((d) => {
        const marker = L.marker([d.latitude || d.lat, d.longitude || d.lng], {
          icon: driverOnlineIcon,
        })
          .addTo(map)
          .bindPopup(
            `<b>🚕 Tài xế Online #${d.driverId}</b><br>Loại xe: ${
              d.vehicleType || vehicleType
            }<br>Cách trung tâm: ${d.distanceKm ? d.distanceKm.toFixed(2) : 0} km`,
          );
        onlineDriverMarkers.push(marker);
      });
    } else {
      addLog(`⚠️ Chưa có tài xế nào Online xung quanh.`);
    }
  } catch (err) {
    addLog(`❌ Không thể tải danh sách tài xế Online!`);
  }
}

// ==========================================
// 2. GỬI TỌA ĐỘ GPS LÊN TRACKING SERVICE
// ==========================================
async function sendDriverLocation() {
  const token = getToken();
  if (!token) {
    alert("Vui lòng dán Bearer Token hoặc đăng nhập tài xế!");
    return;
  }

  const payload = {
    latitude: parseFloat(currentLat.toFixed(6)),
    longitude: parseFloat(currentLng.toFixed(6)),
    vehicleType: document.getElementById("vehicleType")?.value || "BIKE",
  };

  addLog(
    `📡 [POST] Đang đẩy tọa độ (${payload.latitude}, ${payload.longitude}) cho Tài xế...`,
  );

  try {
    const res = await fetchWithAutoRefresh(
      "http://localhost:8080/location-tracking-service/api/v1/location/driver",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
    );

    const data = await res.json();
    if (res.ok) {
      addLog(
        `✅ [GPS SUCCESS] ${data.message || "Đã cập nhật vị trí thành công!"}`,
      );
      setTimeout(loadAllMapData, 500);
    } else {
      addLog(`❌ [LỖI GPS]: ${data.message}`);
    }
  } catch (err) {
    addLog(`❌ [LỖI GPS] Không kết nối được location-tracking-service!`);
  }
}

// ==========================================
// 3. ĐIỀU KHIỂN CÔNG TẮC NGUỒN ONLINE / OFFLINE
// ==========================================
async function onTogglePowerSwitch(checkbox) {
  // Ràng buộc: Trạng thái BUSY không cho phép tắt Online
  if (currentDriverStatus === "BUSY") {
    alert(
      "Bạn đang trong chuyến đi (BUSY), không thể tắt trạng thái hoạt động!",
    );
    checkbox.checked = true;
    return;
  }

  const token = getToken();
  if (!token) {
    checkbox.checked = false;
    addLog("⚠️ Chưa đăng nhập tài khoản Tài xế! Vui lòng đăng nhập...");
    openLoginModal();
    return;
  }

  await toggleDriverStatusApi(checkbox);
}

async function toggleDriverStatusApi(checkbox) {
  try {
    addLog("🔄 Đang gửi yêu cầu chuyển trạng thái (User Service)...");
    const response = await fetchWithAutoRefresh(
      "http://localhost:8080/user-service/api/v1/driver/status",
      { method: "PATCH" },
    );

    const result = await response.json();
    if (response.ok) {
      const newStatus = result.data.status; // ONLINE hoặc OFFLINE
      updateStatusUI(newStatus);
      addLog(`✅ [STATUS SUCCESS] Hiện tại: ${newStatus}`);

      if (newStatus === "ONLINE") {
        sendDriverLocation(); // Bật Online thành công -> Tự động đẩy vị trí GPS
      } else {
        setTimeout(loadAllMapData, 500);
      }
    } else {
      addLog(`❌ [LỖI STATUS]: ${result.message}`);
      if (checkbox) checkbox.checked = currentDriverStatus === "ONLINE";
    }
  } catch (err) {
    addLog(`❌ [LỖI MẠNG]: ${err.message}`);
    if (checkbox) checkbox.checked = currentDriverStatus === "ONLINE";
  }
}

// Cập nhật giao diện Công tắc Nguồn & Nhãn trạng thái
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

// ==========================================
// 4. FETCH BẢO VỆ & AUTO REFRESH TOKEN (401)
// ==========================================
async function fetchWithAutoRefresh(url, options = {}) {
  let token = getToken();

  options.headers = {
    ...options.headers,
    "Content-Type": "application/json",
    Authorization: token
      ? token.startsWith("Bearer ")
        ? token
        : "Bearer " + token
      : "",
  };

  let response = await fetch(url, options);

  if (response.status === 401) {
    addLog("⚠️ Access Token hết hạn! Đang tự động Refresh Token...");
    const refreshed = await doRefreshToken();

    if (refreshed) {
      const newToken = localStorage.getItem("driverAccessToken");
      setTokenInput(newToken);
      options.headers["Authorization"] = "Bearer " + newToken;
      response = await fetch(url, options);
    } else {
      addLog("❌ Session hết hạn! Vui lòng đăng nhập lại.");
      openLoginModal();
    }
  }
  return response;
}

async function doRefreshToken() {
  const refreshToken = localStorage.getItem("driverRefreshToken");
  if (!refreshToken) return false;

  try {
    const res = await fetch(
      "http://localhost:8080/user-service/api/v1/auth/refresh-token",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refreshToken: refreshToken }),
      },
    );

    if (res.ok) {
      const data = await res.json();
      localStorage.setItem("driverAccessToken", data.data.accessToken);
      if (data.data.refreshToken) {
        localStorage.setItem("driverRefreshToken", data.data.refreshToken);
      }
      addLog("✅ [AUTO REFRESH] Cấp lại Token thành công!");
      return true;
    }
  } catch (e) {
    console.error("Lỗi refresh token", e);
  }
  return false;
}

// ==========================================
// 5. QUẢN LÝ MODAL LOGINS & OTP
// ==========================================
function openLoginModal() {
  document.getElementById("loginModal").style.display = "flex";
  document.getElementById("step1").style.display = "block";
  document.getElementById("step2").style.display = "none";
}

function closeLoginModal() {
  document.getElementById("loginModal").style.display = "none";
}

async function requestOtp() {
  const phone = document.getElementById("phoneInput")?.value.trim();
  if (!phone) return alert("Vui lòng nhập Số điện thoại!");

  try {
    const res = await fetch(
      "http://localhost:8080/user-service/api/v1/auth/driver/send-otp",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ phoneNumber: phone }),
      },
    );

    if (res.ok) {
      addLog("📩 Đã gửi mã OTP. Vui lòng kiểm tra tin nhắn!");
      document.getElementById("step1").style.display = "none";
      document.getElementById("step2").style.display = "block";
    } else {
      alert("SĐT không chính xác hoặc không thuộc tài khoản Tài xế!");
    }
  } catch (err) {
    alert("Không thể kết nối đến máy chủ xác thực!");
  }
}

async function submitLoginDriver() {
  const phone = document.getElementById("phoneInput")?.value.trim();
  const otp = document.getElementById("otpInput")?.value.trim();

  try {
    const res = await fetch(
      "http://localhost:8080/user-service/api/v1/auth/login/driver",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ phoneNumber: phone, otp: otp }),
      },
    );

    if (res.ok) {
      const result = await res.json();
      localStorage.setItem("driverAccessToken", result.data.accessToken);
      localStorage.setItem("driverRefreshToken", result.data.refreshToken);

      setTokenInput(result.data.accessToken);
      addLog("🎉 Đăng nhập thành công! Đang tự động bật ONLINE...");
      closeLoginModal();

      // Tự động bật công tắc nguồn và kích hoạt trạng thái ONLINE
      const toggleInput = document.getElementById("driverStatusToggle");
      if (toggleInput) toggleInput.checked = true;
      await toggleDriverStatusApi(toggleInput);
    } else {
      alert("Mã OTP không đúng hoặc đã hết hạn!");
    }
  } catch (err) {
    alert("Đăng nhập thất bại. Vui lòng thử lại!");
  }
}

// ==========================================
// UTILS HELPERS
// ==========================================
function getToken() {
  const inputToken = document.getElementById("driverToken")?.value.trim();
  return inputToken || localStorage.getItem("driverAccessToken");
}

function setTokenInput(token) {
  const inputToken = document.getElementById("driverToken");
  if (inputToken) inputToken.value = token;
}

function addLog(msg) {
  const div = document.getElementById("logs");
  if (!div) return;
  div.innerHTML += `[${new Date().toLocaleTimeString()}] ${msg}<br>`;
  div.scrollTop = div.scrollHeight;
}
