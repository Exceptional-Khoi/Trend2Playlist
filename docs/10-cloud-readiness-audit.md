# Kiểm tra trước triển khai — 05/11/2026

## Phạm vi đã kiểm chứng

- 32 test đạt: logic bốn chức năng, API archive và API pipeline với HTTP giả lập, phân trang Elasticsearch/giải phóng scroll, không fallback sang archive khi ES lỗi, đọc YAML và giới hạn kích thước ConfigMap.
- Python compileall và bash -n cho script triển khai đạt. Đây không phải Kubernetes API schema validation hay deploy thật.
- Giữ một dashboard bốn chức năng; route legacy đã gỡ.

## Các lỗi đã sửa trong đợt rà soát

- Gọi update-code qua bash vì file Git chưa có executable bit.
- Ép LF cho shell scripts để tránh lỗi CRLF khi checkout trên Windows rồi mount Linux.
- Bootstrap chờ cả country crawler, deploy chờ rollout speed layer.
- Country topic được đưa vào raw HDFS, vẫn tách khỏi các bảng điểm Việt Nam.
- Raw JSON trong Elasticsearch lưu bằng text không lập chỉ mục; tránh giới hạn keyword và không tìm kiếm trên nguyên chuỗi JSON.
- Xử lý source_period null khi kiểm tra bài rời BXH.
- Thêm requirements-dev và GitHub Actions để kiểm tra mỗi push/PR.

## Điều kiện nghiệm thu trên GKE khi có cluster

Chưa thể cam kết chạy trơn tru trên Google Cloud chỉ từ unit test. Cần kiểm tra GKE Standard, quota, quyền IAM, node resources, storage class mặc định, PVC Bound, tải image/Maven/PyPI và DNS nội bộ.

1. Kết nối đúng context, kiểm tra node Ready và storage class.
2. Deploy full; tất cả dịch vụ Ready, các job khởi tạo Complete.
3. Bootstrap; charts/playlists/countries hoàn tất, raw HDFS có dữ liệu.
4. Speed layer có query insight_raw hoạt động; Elasticsearch có music-insight-raw.
5. API ở chế độ elasticsearch; country trả cùng kỳ nguồn, feedback web tăng sau thao tác được Kafka xác nhận.
6. Thử phục hồi broker/driver, kiểm tra không mất/trùng sự kiện và checkpoint tiếp tục.

Lịch sử vòng đời/playlist cần tích lũy thật. API hiện đọc raw với giới hạn 200.000 bản ghi; đây là giới hạn hiện hữu, chưa phải kiến trúc serving đạt quy mô dữ liệu lớn. Cần materialized views và kiểm thử tải trước khi chạy lâu dài. Bộ giả lập có thể làm chạm giới hạn này nhanh.

## Chạy lại

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest tests -q
```

Kiểm thử ES riêng (không phải deploy production): đặt ES_URL trỏ ES thử nghiệm, nạp dữ liệu bằng scripts/import-insight-archive.py rồi chạy scripts/check-insights-es.py. Script integration dùng bằng chứng country ngày 05/10/2026, cần archive tương ứng trên máy; archive không nằm trong Git.
