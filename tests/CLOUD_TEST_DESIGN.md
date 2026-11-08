# Chiến dịch 2 — thiết kế Cloud E2E

Ngày thiết kế: 08/11/2026. Source đã sửa ở nhánh `dev`; main GitHub vẫn là phiên bản cũ. SHA, working tree, hash từng file ứng dụng được runner ghi trước/sau; generic image digest không thay thế bằng chứng source ConfigMap. Namespace `music` hiện hữu chỉ được quan sát; user đã cho phép dùng node sẵn có của cụm test `music`.

## Topology và đường dữ liệu thực

Profile test dùng ba Kafka KRaft broker (RF3/minISR2), HDFS NameNode và hai DataNode (replication2), Elasticsearch một node, Spark master/worker/driver, FastAPI và observer. Mọi dịch vụ có state đều riêng namespace, own Kafka DNS, ES index instance và HDFS `/qa/<run-id>`. Không dùng chung state với `music`. Dùng emptyDir có giới hạn; không PVC, disk mới, LoadBalancer, nodepool hay IAM. Đây là profile thu nhỏ: không suy rộng thành HA ES hay node-loss durability của production.

`crawler/src_apple.py` gọi HTTP fixture nội bộ có schema Apple (chỉ URL nguồn được đổi trong process test), producer thật → Kafka thật → hai query độc lập: `ingest_raw` ghi HDFS Parquet, `insight_raw` ghi ES raw → `/api/insights/*`. Bốn insight tính từ ES raw, không từ legacy batch views. Country chart có đường raw này nhưng không nằm trong parsed schemas của batch legacy. `/api/listen` → Kafka → cả raw sinks và `rt_plays` aggregation → ES → API. Test listen seed catalog bằng ES riêng làm precondition, không nhận đó là bằng chứng crawler-to-batch catalog. CE-013 chạy production batch từ HDFS và kiểm chứng REST search riêng.

Recommendation vẫn có query/backend legacy, dashboard hiện hành là bốn insight. Crawler `crawler/` là runtime của deployment; `crawlers/` là các công cụ khác/legacy, không giả định cùng pipeline. Batch cũ và deployment hiện hữu phải được đánh giá theo source đang mount, không gán lỗi trực tiếp cho `dev` khi source khác.

## Oracle và adversarial scenarios

Không import hàm insight để tính expected. Lifecycle được suy từ chuỗi source periods và snapshot completeness; playlist dùng log2 độc lập; country dùng Python sets và rank denominator theo chart size; feedback dùng đếm action và logical event IDs. Kafka topic/partition/offset và raw bytes là oracle transport. Cùng event_id ở hai partition dùng để phân biệt transport duplication hợp lệ với business double-count. Capture cũ đến ở microbatch sau dùng để thử latest-state regression.

Checkpoint commit + offset log chứng minh Spark đã commit qua records cần kiểm tra, không suy luận chỉ từ pod Ready. HDFS đọc Parquet thật qua WebHDFS/pyarrow; ES mget đối chiếu doc ID và raw bytes; API đi HTTP thật từ observer. Image IDs, revision, ConfigMap và mounted file hashes được lưu. Event-time cửa sổ phút tách khỏi Kafka ingest timestamp. Polling bounded, không sleep để đoán readiness. Các case dùng unique keys; thứ tự cố định vì watermark là state dùng chung trong stack test. Điều này được khai báo, không gọi là test-order independence hoàn toàn.

Recovery chỉ restart Pod có đúng namespace, run label và UID; ES outage/latency qua HTTP proxy đến ES thật. Không xóa shared pods/PVC hay sửa production source. Worker, broker, driver, API, DataNode được thử riêng. Pod readiness RTO khác data recovery; không tuyên bố exactly-once chỉ dựa vào số lượng. Controlled load tối đa bốn HTTP worker, 28 event/burst test và tối đa 12 event/short sustained case; báo số đo, không tự đặt SLA.

## Safety, cost, cleanup

Scope JSON bắt buộc explicit context/project/cluster/zone/run; xác minh GKE endpoint và nodepool autoscale disabled trước write. Không dùng ambient gcloud project đang trỏ project khác. Headroom và node metrics guard; dừng khi node memory >90%, CPU >95%, node inventory thay đổi hoặc workload baseline restart/readiness xuống. Quota requests CPU3/memory7Gi, limits CPU8/memory14Gi, pods18; max runtime3600s. Không thay cluster config. Resource journal có UID; cleanup chỉ own resources, kiểm toàn bộ namespace metadata rồi mới xóa namespace, giữ lại nếu còn object không thuộc run. Thu log trước cleanup. Không log Secret data/token.

## Giới hạn thiết kế đã biết

Sau safety stop vì worker hiện hữu OOM, bổ sung bốn bài RO-001..004 dùng namespace hiện hữu theo chế độ read-only: health API/ES, Kafka seek exact offset với auto-commit=false, chart REST/ES consistency, bốn insight HTTP. Chúng kiểm source đang triển khai (hash/digest lưu riêng), không gọi là E2E `dev`, không ghi data/checkpoint. Hai port-forward được đóng khi kết thúc. Thay đổi phạm vi này không bỏ qua safety guard hay restart workload gốc.

Không kiểm node termination/zone outage, corrupted shared checkpoint, IAM changes, disk/PVC loss, long soak, autoscaling hoặc unbounded stress. Không gọi public crawler API. Không chứng minh external source availability/anti-bot, ES multi-node recovery hoặc durable storage khi node mất vì emptyDir. Các case bị chặn tài nguyên phải BLOCKED, không PASS. Watermark exact lateness threshold và future-event poisoning cần query-specific isolation thêm; không suy diễn từ một boundary test. Recommendation, rolling upgrade, simultaneous batch/realtime publication, schema incompatibility và full-network partition chưa được executable coverage đầy đủ; xem matrix deferred rows. Các test cloud default opt-in; normal pytest chỉ skip, không mở kết nối cloud.
