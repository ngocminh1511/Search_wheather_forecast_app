# Quy trình xử lý dữ liệu bão từ JMA (Refined)

## Mục tiêu

Chuẩn hóa quy trình thu thập, xử lý và hợp nhất dữ liệu bão để xuất JSON ổn định cho ứng dụng.

## Bước 1. Quét nguồn và lọc bản tin (Source Polling & Filtering)

- Nguồn feed: https://www.data.jma.go.jp/developer/xml/feed/extra.xml
- Tần suất quét: 10-15 phút/lần.
- Điều kiện lọc:
	- Chọn entry có title chứa Typhoon Information.
	- Ưu tiên bản tin có id hoặc link chứa mã VPTW (ví dụ ..._VPTW60_...xml).
- Khi match điều kiện, tải XML chi tiết từ link trong entry.

## Bước 2. Parse dữ liệu JMA (JMA Parsing)

Trích xuất các trường chính từ XML:

- Tên bão (Name).
- Số hiệu quốc tế (International Number, ví dụ 2603).
- Danh sách điểm theo thời gian.
- Tọa độ tâm bão.

Chuẩn hóa tọa độ:

- Định dạng JMA: +18.2+125.6/
- Kết quả parse: lat = 18.2, lon = 125.6

## Bước 3. Tích hợp dữ liệu cường độ từ GFS (GFS Data Fusion)

Với mỗi mốc thời gian và tọa độ từ JMA, nội suy từ GFS để lấy:

- Wind speed tầng 10m.
- Pressure mực nước biển.

Chuẩn hóa đơn vị:

- Wind: knots sang km/h.
- Pressure: giữ hPa và bổ sung mmHg.

## Bước 4. Hợp nhất lịch sử và trạng thái (Persistent Storage Logic)

### Quy tắc current/past/forecast

- Đọc file hiện có theo mã bão, ví dụ 2603.json.
- Trong dữ liệu XML mới, điểm quan sát đầu tiên được coi là current mới.
- Điểm đang là current cũ trong file trước đó phải chuyển thành past.
- Tất cả forecast cũ phải xóa và thay bằng forecast mới từ XML hiện tại.

### Kết quả trạng thái sau mỗi lần cập nhật

- past: danh sách tăng dần theo thời gian.
- current: đúng duy nhất 1 điểm.
- forecast: chỉ giữ bộ dự báo mới nhất.
