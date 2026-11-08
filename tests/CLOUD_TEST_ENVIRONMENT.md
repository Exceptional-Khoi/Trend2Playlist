# Môi trường và cách chạy

Python 3.12; cài `requirements-dev.txt`. Host cần PyYAML, pytest; Google Cloud CLI và kubectl có quyền trong cụm test. Observer tự cài requests/kafka-python/pyarrow17 trong Pod test; Spark tải đúng connector theo production submit.sh. Không cần API key crawler thật. Không dùng file archive ngoài git.

Tài liệu public này dùng cấu hình mẫu; scope thực tế và log/metadata được giữ trong báo cáo cục bộ. Chỉ chạy khi chủ môi trường xác nhận đây là test cluster và cho phép dùng node sẵn có. Mọi command phải explicit project/context. Kiểm lại auth/context/capacity trước mỗi run. Namespace phải mới, `music-e2e-<run-id>`, tuyệt đối không reuse state hoặc nhận resource cũ.

```json
{"project":"qa-example-project","cluster":"music","location":"example-zone","context":"gke_qa-example-project_example-zone_music","namespace":"music-e2e-20261108-example","run_id":"20261108-example","approved_existing_nodes":true,"expected_nodes":3,"max_runtime_seconds":3600}
```

Lưu JSON thành scope mới cho mỗi run. Các lệnh tại checkout `dev`:

```text
python scripts/run-cloud-tests.py local --report-dir test_reports/cloud_e2e/local
python scripts/run-cloud-tests.py gke-e2e --scope scope.json --collect-only
python scripts/run-cloud-tests.py gke-e2e --scope scope.json
python scripts/run-cloud-tests.py gke-readonly --scope scope.json
python scripts/run-cloud-tests.py gke-reliability --scope scope.json --allow-destructive
python scripts/run-cloud-tests.py gke-performance --scope scope.json
python scripts/run-cloud-tests.py gke-all --scope scope.json --allow-destructive
```

Mỗi command tạo và cleanup stack riêng; scope run ID phải đổi khi chạy lại. Runner ghi raw checkpoints JSONL, source hashes, Kubernetes identity/logs, manifests, resource journal, pytest JUnit và results. `finally` thực hiện cleanup; nếu process bị kill, dùng journal và UID để kiểm ownership trước thao tác phục hồi. Không dùng deploy.sh hoặc script fault cũ vì hardcode namespace `music`.

Mặc định `pytest tests` không chạy cloud. Local baseline không coi integration thiếu service là pass. Không ước tính chi phí từ bảng giá cũ: không thêm billed resource; incremental compute trên node đã cấp thường không thêm node-hour nhưng network egress/dependency download và logging có thể phát sinh, chưa có billing attribution thì NOT MEASURED.
