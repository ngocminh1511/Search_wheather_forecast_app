9h25 bắt đàu tải
Mưa nâng cao: dự báo trong 24h, 3h cập nhật 1 lần
Mưa cơ bản: dự báo trong vòng 24h, 3h cập nhật 1 lần

Nhiệt độ: Dự báo trong vòng 72h, 
    1 ngày đầu: 1h cập nhật 1 lần 
    2 ngày sau: 3h cập nhật 1 lần

Độ phủ của mây: Lưu lại quá khứ, không dự báo tương lai, 3h cập nhật 1 lần

Dự báo độ dày của tuyết: dự báo trong khoản 9-10 ngày,
    2 ngày đầu: 3h cập nhật 1 lần 
    42-54h sau : 6h cập nhật 1 lần
    54h sau -> hết: 12 h cập nhật 1 lần

Hoạt ảnh gió: Dự báo trong vòng 16 ngày, 
    5 ngày đầu: 1h cập nhật 1 lần 
    11 ngày sau: 3h cập nhật 1 lần 
    => gồm nhiều mức độ cao khác nhau
        Mặt đất: 30,50,100m
        Trên mực nước biển: 1000,4200,9200, 10400, 11800m


Đường link tải dữ liệu

1.	Bản đồ Nhiệt độ (Cảm nhận thực tế)
Tải đúng 1 biến (TMP) ở 1 tầng (2m).
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_2_m_above_ground=on&var_TMP=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos

2.	Bản đồ Mưa cơ bản (Tổng lượng mưa)
Tải đúng 1 biến (APCP) ở bề mặt.
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_surface=on&var_APCP=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos

3.	Bản đồ Mưa nâng cao (Loại mưa, Tốc độ)
Tải 3 biến kết hợp (PRATE, CRAIN, CSNOW) ở bề mặt.
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_surface=on&var_PRATE=on&var_CRAIN=on&var_CSNOW=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos


4.	Bản đồ Độ phủ mây (Tổng hợp 2D)
Lấy tổng lượng mây toàn bộ cột khí quyển.
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_entire_atmosphere=on&var_TCDC=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos

5.	Bản đồ Mây tách lớp (Cho 3D / Nâng cao)
Lưu ý: Để tránh tổ hợp chéo lỗi như đã phân tích, bạn chia làm 3 link riêng biệt cho 3 tầng.
Mây tầng thấp:
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_low_cloud_layer=on&var_LCDC=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos

Mây tầng trung:
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_middle_cloud_layer=on&var_MCDC=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos

Mây tầng cao:
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_high_cloud_layer=on&var_HCDC=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos

6.	Bản đồ Độ dày tuyết
Tải 1 biến ở bề mặt.
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_surface=on&var_SNOD=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos

Bản đồ Hoạt ảnh gió (Tách lẻ theo tầng)
Dưới đây là ví dụ tách cho 2 tầng gió cụ thể. Cần lấy U và V đi theo cặp.
•	Link Gió mặt đất (Ví dụ lấy cao độ 50m):
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_50_m_above_ground=on&var_UGRD=on&var_VGRD=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos
•	Link Gió trên cao (Ví dụ lấy tầng ~4,200m / 600mb):
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t[HH]z.pgrb2.0p25.f[FFF]&lev_600_mb=on&var_UGRD=on&var_VGRD=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%2Fgfs.[YYYYMMDD]%2F[HH]%2Fatmos

 