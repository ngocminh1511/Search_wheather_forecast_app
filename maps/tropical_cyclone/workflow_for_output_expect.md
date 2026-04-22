# Quy trình xử lý dữ liệu bão (Cyclone Data Processing Workflow)

Tài liệu này là bản chốt cuối để triển khai code trả dữ liệu bão cho ứng dụng di động dưới dạng JSON chuẩn.

## 1. Xác định khu vực (Basin Identification)

Tùy theo mã khu vực, hệ thống lấy dữ liệu từ nguồn tương ứng:

- NA (Bắc Đại Tây Dương) và EP (Đông Thái Bình Dương): lấy trực tiếp từ NHC (National Hurricane Center).
- WP (Tây Bắc Thái Bình Dương, gồm Biển Đông):
	- Lấy quỹ đạo bão (track) từ JMA.
	- Đồng bộ theo lưới GFS để trích xuất thêm gió (wind) và áp suất (pressure) tại từng mốc.

## 2. Phân loại điểm dữ liệu (Point Categorization)

Mỗi điểm trong track phải có trường type theo advisory_time:

- past: thời điểm trước hiện tại.
- current: điểm gần hiện tại nhất, chỉ có đúng 1 điểm.
- forecast: các điểm tương lai (+12h, +24h, +48h, +72h, +96h, +120h).

## 3. Chuyển đổi đơn vị (Unit Conversion)

- Gió: knots sang km/h với công thức $kmh = knots \times 1.852$.
- Áp suất gốc: hPa.
- Áp suất hiển thị bổ sung: mmHg với công thức $mmHg = hPa \times 0.75$.

## 4. Logic gán icon (Icon Mapping)

Áp dụng cho trường icon_tag dựa trên wind_speed (WS, knots) và type:

| Điều kiện gió | type | icon_tag |
|---|---|---|
| WS < 34 | past | TDgray.png |
| WS < 34 | current hoặc forecast | TD.png |
| 34 <= WS < 64 | past | TSgray.png |
| 34 <= WS < 64 | current hoặc forecast | TS.png |
| WS >= 64 | past | TYgray.png |
| WS >= 64 | current hoặc forecast | TY.png |
| Không phân loại được | mọi type | LOW.png |

## 5. Tính toán bổ sung (Advanced Calculations)

### 5.1. forward_speed

- Ý nghĩa: địa tốc giữa hai điểm liên tiếp.
- Công thức: khoảng cách giữa điểm n và n-1 chia cho chênh lệch thời gian.
- Khuyến nghị: dùng Haversine để tính khoảng cách theo lat/lon.

### 5.2. direction

- Ý nghĩa: hướng di chuyển (N, NNE, NE, ENE, E, ESE, SE, SSE, S, SSW, SW, WSW, W, WNW, NW, NNW).
- Cách làm: tính bearing từ điểm n-1 sang n, sau đó ánh xạ về 16 hướng.

### 5.3. cone_radius_km (Bản chốt để code)

Quy tắc chung:

- Nếu type là past hoặc current: cone_radius_km = 0.
- Nếu type là forecast:
	- Nếu nguồn có sẵn radius lỗi dự báo: dùng trực tiếp.
	- Nếu không có: dùng bảng hardcode cố định bên dưới.

Bảng cone cố định cuối cùng:

| Lead time | cone_radius_km |
|---|---|
| +12h | 45 |
| +24h | 80 |
| +48h | 150 |
| +72h | 225 |
| +96h | 325 |
| +120h | 450 |

Quy tắc ngoại lệ cho lead time không nằm đúng mốc:

- Nội suy tuyến tính giữa 2 mốc gần nhất.
- Nhỏ hơn +12h: gán 45.
- Lớn hơn +120h: gán 450.

## 6. Trường dữ liệu đầu ra tối thiểu cho mỗi điểm

Mỗi phần tử track nên có tối thiểu:

- advisory_time
- lat, lon
- type
- wind_kts, wind_kmh
- pressure_hpa, pressure_mmhg
- icon_tag
- forward_speed
- direction
- cone_radius_km

## 7. Thứ tự xử lý khuyến nghị

1. Đọc dữ liệu nguồn theo basin.
2. Chuẩn hóa timeline và gắn type.
3. Chuyển đổi đơn vị gió và áp suất.
4. Tính forward_speed và direction.
5. Gán cone_radius_km theo quy tắc chốt.
6. Gán icon_tag theo bảng mapping.
7. Xuất JSON đầu ra.