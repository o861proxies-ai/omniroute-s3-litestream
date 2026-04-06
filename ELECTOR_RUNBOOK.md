# Elector Runbook (Leader/Follower)

Tài liệu này mô tả **luồng hoạt động thực tế** của `elector`, cách cấu hình để chạy đúng luồng đó, và checklist kiểm tra sau khi deploy.

---

## 1) Luồng làm việc chính của `elector`

### Mục tiêu
- Chỉ có **1 instance Leader** tại một thời điểm.
- Chỉ Leader được chạy các service ghi/serve chính:
  - `litestream`
  - `omniroute`
  - `cloudflared`
- Trên node follower, tất cả service trong compose (trừ service giữ lại như `elector`) đều phải dừng.
- Các instance còn lại là **Follower** (đứng chờ), không chạy các service trên.

### Luồng chi tiết

1. **Elector khởi động**
   - Đọc `RTDB_URL`, `COMPOSE_PROJECT_NAME`, `LEADER_LOCK_TTL`, `HEARTBEAT_INTERVAL`, `INSTANCE_ID`.
   - Chuẩn hóa project name để match Docker label.
   - Sinh `INSTANCE_ID` nếu chưa có.

2. **Init an toàn (rất quan trọng)**
   - Ngay lúc start, elector đọc danh sách service từ `docker compose config --services`.
   - Elector sẽ stop toàn bộ service không nằm trong `ELECTOR_KEEP_SERVICES` (mặc định chỉ giữ `elector`).
   - Mục tiêu: không để follower vô tình replicate lên S3.

3. **Election loop**
   - Elector đọc lock trên Firebase RTDB.
   - Nếu lock trống/hết hạn, dùng conditional PUT (`If-Match ETag`) để giành lock atomically.

4. **Nếu thắng election → Leader**
   - Start theo thứ tự:
     1) `litestream`
     2) chờ `litestream` healthy (tối đa 180s)
     3) `omniroute`
     4) `cloudflared`
   - Sau đó heartbeat định kỳ để gia hạn lock.

5. **Nếu thua election → Follower**
   - Giữ toàn bộ service không bắt buộc ở trạng thái dừng (tự động theo compose, trừ các service trong `ELECTOR_KEEP_SERVICES`).
   - Chỉ theo dõi leader hiện tại và thử giành lock khi lock hết hạn.

6. **Khi Leader mất lock / shutdown**
   - Demote về follower:
     - stop toàn bộ service không thuộc `ELECTOR_KEEP_SERVICES`
     - stop chính container `elector` của node hiện tại (mặc định bật)
   - Release lock (nếu đang giữ lock).

> Có thể tùy chỉnh service được giữ lại bằng biến:
> `ELECTOR_KEEP_SERVICES=elector,service_khac`
>
> Có thể tắt hành vi tự stop elector bằng:
> `ELECTOR_STOP_SELF_ON_FOLLOWER=false`

### Luồng bảo vệ dữ liệu trong `litestream/startup.sh`

Khi `litestream` được Leader start:
- Nếu local DB đã có: bỏ qua restore.
- Nếu local DB chưa có:
  - check snapshot trên S3.
  - nếu có snapshot thì **bắt buộc restore thành công**.
  - restore fail => **exit 1** (không cho chạy tiếp với DB rỗng).

Điều này ngăn trường hợp ghi đè dữ liệu cũ khi S3 credential/network lỗi.

---

## 2) Cấu hình đúng để chạy theo luồng trên

## 2.1 Biến môi trường bắt buộc (`.env`)

### Leader election
- `RTDB_URL`
  - URL Firebase RTDB (có thể kèm `?auth=TOKEN`).
- `COMPOSE_PROJECT_NAME`
  - Nên cố định, giống nhau trong 1 deployment target.
- `INSTANCE_ID` (khuyến nghị)
  - Nên inject từ CI để debug dễ (vd: run_id-attempt-runner).
- `LEADER_LOCK_TTL` (optional, default 30)
- `HEARTBEAT_INTERVAL` (optional, default 10)

### Litestream/S3
- `LITESTREAM_BUCKET`
- `LITESTREAM_ACCESS_KEY_ID`
- `LITESTREAM_SECRET_ACCESS_KEY`
- `SUPABASE_PROJECT_REF`

### Ứng dụng
- `OMNIROUTE_PORT` (nếu cần đổi port)
- `NEXT_PUBLIC_BASE_URL` (nếu cần)

## 2.2 Yêu cầu `docker-compose.yml`

- `elector` phải mount docker socket:
  - `/var/run/docker.sock:/var/run/docker.sock`
- Managed services phải để `restart: "no"`:
  - `litestream`, `omniroute`, `cloudflared`
- `litestream` dùng `entrypoint: ["/bin/sh", "/startup.sh"]`

> Lưu ý: Nếu để Docker tự restart các managed services, sẽ phá vỡ control plane của elector và có thể gây race condition.

## 2.3 Network và quyền truy cập

- Container phải truy cập được:
  - Firebase RTDB endpoint (`RTDB_URL`)
  - Supabase S3 endpoint (`${SUPABASE_PROJECT_REF}.supabase.co/storage/v1/s3`)
- Cloudflare tunnel credentials phải hợp lệ.

---

## 3) Cách kiểm tra hệ thống chạy đúng luồng

## 3.1 Kiểm tra tĩnh trước khi chạy

1. Validate compose:
```bash
docker compose config
```

2. Kiểm tra biến môi trường quan trọng:
```bash
cat .env | sed -n '1,200p'
```

3. Kiểm tra script hợp lệ cú pháp:
```bash
bash -n services/elector/elector.sh
sh -n litestream/startup.sh
```

## 3.2 Kiểm tra runtime 1 instance

1. Start stack:
```bash
docker compose up -d --build
```

2. Xem log elector:
```bash
docker logs -f <project>-elector
```
Kỳ vọng thấy:
- message thắng election hoặc follower theo dõi leader
- nếu leader: start `litestream` trước, sau đó `omniroute`, `cloudflared`

3. Kiểm tra trạng thái container:
```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```
Kỳ vọng:
- Leader instance: cả 3 managed services running
- Follower instance: managed services stopped

## 3.3 Kiểm tra failover nhiều instance

Giả sử có 2 máy/runner chạy cùng cấu hình lock key:

1. Xác định leader hiện tại qua log elector.
2. Tắt leader:
```bash
docker stop <project>-elector
```
hoặc dừng toàn bộ stack trên leader.

3. Theo dõi follower:
```bash
docker logs -f <project>-elector
```
Kỳ vọng:
- follower giành lock và chuyển sang leader
- start lại theo đúng thứ tự `litestream -> omniroute -> cloudflared`

## 3.4 Kiểm tra bảo vệ mất dữ liệu (restore fail)

Mục tiêu: xác nhận hệ thống **không** chạy với DB rỗng khi S3 có replica nhưng restore lỗi.

Gợi ý test:
1. Đảm bảo S3 đã có snapshot cũ.
2. Xóa local DB trên test env.
3. Cố tình cấu hình sai 1 credential S3.
4. Start lại leader.

Kỳ vọng:
- `litestream/startup.sh` báo restore fail và exit 1.
- `omniroute` không được start thành công như trạng thái leader fully active.

---

## 4) Checklist nhanh (copy/paste)

- [ ] `RTDB_URL` đúng và truy cập được.
- [ ] Các node dùng cùng `LEADER_LOCK_KEY` theo cùng deployment target.
- [ ] `litestream|omniroute|cloudflared` đều `restart: "no"`.
- [ ] `elector` có mount docker socket.
- [ ] `litestream` chạy qua `startup.sh`.
- [ ] Log cho thấy chỉ 1 leader tại một thời điểm.
- [ ] Failover test qua stop leader thành công.
- [ ] Test restore fail không làm app chạy với DB rỗng.
