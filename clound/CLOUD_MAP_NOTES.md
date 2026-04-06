# Ghi chu trien khai ban do may Viet Nam

## Muc tieu
- Tao giao dien may nang cao tren ban do cho khu vuc Viet Nam.
- Ket hop 3 tang may: low, mid, high.

## Trinh tu thuc hien
1. Nap du lieu tu file GRIB `../gdas.t06z.pgrb2.0p25.f000` bang `cfgrib`.
2. Tu dong tim bien du lieu cho 3 tang may:
   - Uu tien: `lcdc`, `mcdc`, `hcdc`
   - Fallback: `lcc`, `mcc`, `hcc`, hoac `tcc` neu can
3. Chuan hoa du lieu ve khoang `[0, 1]` de de ket hop layer.
4. Cat du lieu theo pham vi Viet Nam:
   - Latitude: 8 -> 24
   - Longitude: 102 -> 110
5. Xay dung 4 overlay tren Folium:
   - Cloud Low
   - Cloud Mid
   - Cloud High
   - Cloud Composite (Enhanced)
6. Thuc hien custom giao dien may nang cao:
   - Tron low/mid/high theo trong so
   - Them texture song sin nhe
   - Lam muot bang Gaussian blur
7. Luu ket qua ra file HTML: `cloud_vietnam_advanced_map.html`.
