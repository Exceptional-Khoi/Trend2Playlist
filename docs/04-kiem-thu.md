# Kiểm thử hiện hành

## Offline mặc định

```bash
python -m pip install -r requirements-dev.txt
python -m pytest tests -q
python -m compileall -q serving crawler spark
find scripts -name '*.sh' -exec bash -n {} \;
```

Unit/property/API tests dùng filesystem tạm và dependency giả lập khi cần, không yêu cầu API key thật. Test HTTP chạy qua FastAPI; browser tests có ghi rõ khi intercept API. Test browser thiếu Playwright/Edge hoặc archive không được commit sẽ SKIP có lý do.

## Phân nhóm

| Nhóm | Phạm vi |
|---|---|
| `tests/test_*.py` | Model, crawler, deployment, checkpoint config và tính năng hiện tại |
| `tests/adversarial/` | Input biên, schema sai, property/metamorphic, security, API faults và concurrency/memory |
| `tests/qa_phase1/` | Regression bổ sung: report storage, credential logging, raw errors, browser DOM, observer contracts |
| `tests/cloud/` | E2E/reliability với dịch vụ thật, opt-in và scope riêng |

Các hàm legacy như playlist exposure/Jaccard vẫn có regression vì còn là code phân tích; không coi chúng là response schema của bảng membership/bản đồ hiện tại.

## Cloud tests

Dùng `scripts/run-cloud-tests.py --help`, đọc `tests/CLOUD_TEST_ENVIRONMENT.md` và cung cấp scope đã xác nhận. Không tự chạy cluster, node pool hoặc test destructive trên workload chung. Scope phải cô lập topics/indices/storage, không chỉ tách namespace.

`pytest tests` thông thường không chạy cloud cases. SKIP/BLOCKED không phải PASS E2E. Test public feed cũng opt-in, có giới hạn request. Không kết luận Kafka/Spark recovery hoặc exactly-once từ mock.

## Bản sửa quan trọng đã có bằng chứng

- Snapshot thiếu không bị coi là rời BXH hoàn chỉnh; source period khác processing/crawl time.
- Playlist membership hiện tính cả unknown/chart mirror/playlist nhỏ và hiện URL từ nguồn.
- Query raw theo feature/time trước raw cap; ES lỗi không fallback archive.
- Hai query nặng không đọc/phân tích chồng working set trong cùng API process. Health/static vẫn phục vụ; test trùng request kiểm tra cache sau khi chờ.
- Snapshot không giữ nhiều bản sao JSON; test kiểm tra allocation và tie order độc lập.
- Untrusted text được kiểm tra ở DOM; asset dashboard cũ, `/legacy` và traversal phải 404.

Lượt cloud có giới hạn ngày 10/11/2026: hai request countries/lifecycle cùng một pod đều 200, peak cgroup khoảng 642,5MiB/1GiB, không restart trong khoảng 80 giây. Đây không phải SLA hay soak test. Chi tiết và giới hạn ở [bản cập nhật hiện tại](16-cap-nhat-music-pulse-2026-11-10.md).

CI chạy toàn suite offline, compile và bash syntax. Coverage cao hoặc một lần toàn test PASS không chứng minh production-ready; còn cần materialized views, kiểm thử dài hạn và đánh giá chất lượng nhãn AI.
