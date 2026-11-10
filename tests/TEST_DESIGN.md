# Chiến dịch kiểm thử đối kháng — thiết kế, 08/11/2026

## Đối tượng và hai pha

Baseline lịch sử của chiến dịch phát hiện lỗi là **main sạch, commit `4ab6c756adb36ba96ec7c67883ce448bff928817`**. Checkout hiện tại đã có các thay đổi sản phẩm sau chiến dịch; không còn là baseline sạch. Báo cáo/bằng chứng trước sửa được giữ riêng, không ghi đè bằng kết quả mới. Hợp đồng UI/API hiện hành nằm trong [bốn tính năng](../docs/09-four-features-implementation.md). Các phần inventory/oracle lịch sử bên dưới giải thích test model/legacy, không phải danh sách endpoint hay màn hình đang chạy.

PHA 1: đọc tài liệu/code, viết test và matrix. PHA 2 chỉ bắt đầu sau khi lưu `test_reports/PHASE1_COMPLETE.json`. Khi PHA 2 tìm thấy test sai, lưu lý do điều chỉnh và bằng chứng; không đổi oracle để che bug. Có thể bổ sung regression nhỏ để xác minh phát hiện ở PHA 2, phải ghi rõ đó là bước đào sâu.

Mã test nằm trong `tests/adversarial/`. Mặc định source là root repository nếu có `serving/insights.py`; biến `QA_SOURCE_ROOT` cho phép chọn checkout khác. Cài `requirements-dev.txt` rồi chạy `python -m pytest tests`. Các test cũ của main được giữ nguyên. Dependency thật yêu cầu manifest container local thuộc quyền sở hữu chiến dịch, nếu không thì đánh dấu BLOCKED. Performance artifacts mặc định dùng tmp_path khi không chạy qua report harness.

## Khảo sát hệ thống thực tế

Đã đối chiếu README, PROJECT_PLAN, tài liệu 07/09/10, workflow, API và các module liên quan. Inventory AST, interfaces và SHA-256 của production source nằm trong `test_reports/SOURCE_INVENTORY.json`.

| Nhóm | Vai trò và trạng thái thực tế | Input → output | Dependency / điểm rủi ro |
|---|---|---|---|
| `serving/insights.py` | Logic hiện hành cho 4 chức năng | raw dict có record_type, platform_id, source_period, timestamp → kết quả JSON | Tính đầy đủ snapshot, phân biệt kỳ nguồn/crawl, dedup, validation số, namespace ID |
| `serving/api.py` | FastAPI archive/ES; ba insight và API bình luận | HTTP → JSON | Validation, ES scroll/cleanup, dependency timeout, memory cap; `/api/listen` hiện phải 404 |
| `serving/static/insights.*` | Dashboard duy nhất trên main | API JSON → DOM và form thao tác | XSS, trạng thái lỗi, kết quả rỗng, stale request; dùng textContent |
| API compatibility | `/api/provinces`, trending, recommend và nhiều JSON route cũ vẫn tồn tại | ES batch/realtime → JSON | Không suy ra tất cả API cũ đã xóa chỉ vì dashboard cũ bị gỡ; `/legacy` và static cũ phải 404 |
| `crawler/common.py` | Session retry, record builders và sinks hiện hành | HTTP/record → Kafka, stdout hoặc JSONL | Timeout, dữ liệu thiếu, async delivery failure, serialization, tên file topic |
| `crawler/src_*` | Apple, Spotify/Kworb, Zing, YouTube, Trends | HTTP JSON/HTML → chart/playlist/comment/trends records | Schema drift, rate-limit, period metadata, malformed HTML, pagination |
| `crawler/run_crawler.py` | Entrypoint CronJob và isolation theo nguồn | job/sources/env → sink + exit code | Partial success/strict mode; empty source; lỗi sink |
| `crawler/simulator.py` | Tạo events giả lập | Catalog + cấu hình → music.events | Phải tách source simulator khỏi web; không phải người nghe thật |
| `crawler/survey_*.py` | Công cụ thăm dò nguồn, không phải luồng CronJob chính | Nhiều endpoint thử → bằng chứng thăm dò | Inventory đầy đủ nhưng không bao phủ mọi nhánh survey trong chiến dịch |
| `crawlers/` và `pipeline/` | Hệ local/legacy vẫn có code chạy được; `run_all.py` dùng đường này | base_record/event_time, output JSONL → canonical và province estimates | Khác schema Kafka hiện hành; fallback synthetic; entity false merge; overwrite/order dependence |
| `shared/textnorm.py` | Chuẩn hóa/key/phân tỉnh dùng bởi crawler, Spark, API | Unicode/title/artists/comment → key/mentions | Latin-only slug, version distinctions, artist splitting, false-positive địa phương |
| `shared/provinces.py` | Danh mục tỉnh/alias | Tên/alias → province_code | Giao thoa tên bài và địa danh |
| `spark/speed_layer.py` | Streaming; main có insight_raw ngoài 5 query cũ | Kafka → HDFS raw, ES raw/realtime/recommendations | Stateful foreachBatch, checkpoint, watermark, raw volume, replay |
| `spark/jobs_common.py` | Schemas, ES writer, versioned view helpers | DataFrame/raw/view → HDFS/ES | Partial failure, idempotent ID config, non-atomic publication, cleanup race |
| `spark/batch_*` | Batch view/similarity | HDFS → versioned view và ES | Không chứng minh thuật toán Spark thực chỉ bằng mock; cần Spark runtime |
| `schemas/` | Hai **ví dụ JSON**, không phải JSON Schema machine-validating | Legacy user_event/canonical_track examples | Không lấy ví dụ `play|like|skip` làm một action thật; schema không đồng nhất với Kafka events |
| `k8s/`, scripts | Standard Kubernetes deployment, code qua ConfigMap | YAML/Bash → cluster | Namespace/reference, probes, resources, injected env ports, LF, dependency order |
| CI | pytest, compileall, bash -n | Push/PR → checks | Tài liệu đã thừa nhận YAML parse không phải API schema; campaign bổ sung offline schema validation |

Tài liệu 07 giữ thiết kế ý tưởng ban đầu và được gắn nhãn tham khảo. README/PROJECT_PLAN/tài liệu 09 hiện mô tả bốn tính năng Music Pulse, trong đó có phân tích bình luận bằng LLM. Số test/tích hợp mà báo cáo lịch sử kể lại không được đưa vào kết quả chạy mới; lấy collection/JUnit mới làm bằng chứng.

## Oracle và bất biến

- **Vòng đời:** 3 kỳ nguồn được quan sát mới đủ luật tăng/giảm; stable có biên độ ≤3. Chỉ snapshot đủ hạng, đủ ID mới là bằng chứng rời BXH. Không dùng ngày crawl để chế tạo kỳ nguồn.
- **Playlist:** chỉ editorial đã xác nhận; playlist ID riêng biệt; score tính độc lập bằng `math.fsum(1/log2(p+1))`; cùng nguồn, cùng chart family ở mốc 1/3/7 ngày. Không suy diễn nhân quả.
- **Country:** oracle dùng tập Python độc lập với implementation: giao/hợp, top N, full-chart denominator, hạng chuẩn hóa. Missing không là zero. Stream share hợp lệ phải finite và trong [0,1].
- **Feedback:** reference counter theo event_id, action, source; skip/(play+skip), like-only không có mẫu số. Timestamp kiểm tra ở mốc cố định và UTC+7, không dùng wall-clock để tạo expected value.
- **HTTP:** TestClient chạy handler ASGI thật. Dependency HTTP có MockTransport ở test offline; không mock cả handler. Test limit vẫn decode thật từng raw record, chỉ thay bước phân tích sau read-limit để cô lập hợp đồng phân trang.
- **State/ES helper:** compile/call function AST thực với doubles, ghi rõ unit contract. Không gắn nhãn Spark integration hay exactly-once cho kết quả này.
- **Kubernetes:** parsing/reference và OpenAPI schema là hai mức khác nhau. Không gọi apply/delete, không chạy script fault tolerance lên GKE.
- **Performance:** đo perf_counter, process CPU, tracemalloc; không tự đặt SLA hoặc bịa throughput. Dataset bounded, không stress GCP.

## Kỹ thuật đối kháng

Test cố định gồm equivalence partitions, biên, decision tables và state transitions. Hypothesis sinh tối đa 900 ví dụ cho 5 thuộc tính (200+150+200+150+200; shrinking có thể chạy thêm), dùng seed được lưu và không phụ thuộc archive ngoài Git. Function-scoped fixtures trong property tests chỉ trả module stateless hoặc đường dẫn; health-check fixture được tắt có chủ đích, không che side effect.

Metamorphic tests kiểm tra duplicate replay, đảo thứ tự, canonical Unicode và không mutate input. Differential tests tự tính công thức. Faults được tiêm vào HTTP, Kafka producer double, ES writer và filesystem. Concurrency dùng ThreadPoolExecutor cùng Event với deadline; không sleep để đoán thứ tự. Network ngoài loopback bị chặn trong offline tests.

Browser security test chạy DOM/JS thật, nhưng mọi HTTP route được fulfill bằng fixture. Đây là kiểm thử giao diện với backend giả, không phải E2E hệ dịch vụ. Không gửi fixture tới API đang port-forward trên máy.

## Các trường hợp cần quyết định yêu cầu trước khi gọi là bug

- `B06`: record hiện hành không bắt buộc `playlist_size`; yêu cầu kiểm tra vị trí vượt declared size cần chốt schema.
- `D03`: conflict cùng event_id nhưng khác payload/source chưa có chính sách first-wins/reject/latest; tính bất biến thứ tự là tiêu chí mong muốn, chưa có quy tắc tie-break chính thức.
- Một số khác biệt metadata ở resolver legacy có thể cần chính sách lựa chọn ID mới nhất/best rank; không thổi số fail thành số bug độc lập.

Các test này được giữ và báo riêng nếu fail; không sửa expected chỉ để suite xanh. Những input rõ ràng vi phạm dữ liệu được phân loại là data-contract/quality gap nếu boundary không hề cam kết nhận loại đó, thay vì gọi mọi exception là lỗi nghiệp vụ Critical.

## Môi trường và integration

Không dùng cụm GCP hiện có, kể cả để chèn event thử, restart hay tiêm lỗi. Không tạo tài nguyên cloud tính phí. PHA 2 sẽ kiểm tra Docker/local packages read-only. Nếu có Docker phù hợp, chỉ được tạo container riêng, có label campaign, cổng loopback và giới hạn RAM. Integration chỉ được chạy khi ownership manifest xác minh container thuộc campaign; không mặc định localhost:9200 là môi trường test.

R01/R02 kiểm tra ES thật; R03 Kafka thật; R04 Spark file-source/checkpoint thật nếu có PySpark và runtime phù hợp. R05 là **danh sách kế hoạch cluster NOT RUN**, không phải test fault đã thực thi: full E2E, outage broker/HDFS, consumer restart, late/watermark, corrupted checkpoint, schema evolution và state growth. Một bài test file-source Spark không chứng minh pipeline Kafka→HDFS→ES exactly-once.

Fault semantics cần cluster: kiểm soát input UUID, checkpoint/offset trước-sau, số record HDFS/ES/API, inject failure vào từng boundary, rồi đối chiếu không mất/trùng. Các script fault/scalability hiện có có lệnh delete/scale không có guard context test; chỉ audit, không chạy.

## Coverage, mutation, lặp lại và bằng chứng

Đo statement/branch coverage trên **toàn bộ** sáu nhóm source (serving/crawler/crawlers/pipeline/shared/spark), kể cả file không import, để không làm đẹp mẫu số. Phân biệt hit do dependency double và real integration trong báo cáo.

Mutation giới hạn 5 thay đổi in-memory ở công thức/hướng so sánh; chỉ dùng test baseline đã pass, lưu kết quả từng subprocess. Không sửa source production để tạo mutant. Kết quả không phải mutation score toàn repo.

Chạy baseline, các nhóm test mới, full suite, rồi lặp subset quan trọng bằng seed khác và đảo thứ tự node IDs. Artifact lưu nodeid/ID, input parametrized, expected, traceback, elapsed và JSON report. Hash source trước/sau phải khớp. Unique bug được dedup theo root cause, không theo số param fail; lỗi test, environment và yêu cầu chưa rõ có mục riêng.
