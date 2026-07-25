# Kiến trúc mục tiêu

Đây là bản đồ để định hướng các bài thực hành. Chưa có luồng nào được cài đặt
trong source code.

```text
Client
  |
  v
Order Service
  |
  v
Kafka ---> Inventory Service
  |             |
  |             v
  +--------> Payment Service
  |             |
  +--------> Notification Service
  |
  +--------> Analytics Service
```

## Nguyên tắc đã chuẩn bị sẵn

- `orderId` sẽ là message key để giữ thứ tự sự kiện của cùng một đơn.
- Event topic có 3 partition để thực hành consumer group và rebalance.
- Broker tắt auto-create topic để lỗi chính tả không âm thầm sinh topic mới.
- Retry topic giữ dữ liệu 3 ngày.
- DLQ giữ dữ liệu 30 ngày.
- `order.state.v1` dùng log compaction để giữ trạng thái mới nhất theo key.
- Producer được cấu hình idempotence và `acks=all`.
- Consumer tắt auto commit và chuẩn bị manual acknowledgement.
- Mỗi service có database riêng để có thể thực hành Outbox và idempotent
  consumer sau này.

## Thứ tự triển khai dự kiến

1. `order-service` phát `order.created.v1`.
2. `inventory-service` xử lý tồn kho và phát kết quả.
3. `payment-service` xử lý thanh toán giả lập.
4. `order-service` hoàn tất hoặc hủy đơn.
5. `notification-service` gửi thông báo giả lập.
6. `analytics-service` tổng hợp dữ liệu bằng Kafka Streams.
7. Bổ sung retry, DLQ, replay, Outbox và schema evolution.

