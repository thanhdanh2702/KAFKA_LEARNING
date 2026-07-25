# Kafka Order Platform

Khung project thực hành Apache Kafka qua bài toán xử lý đơn hàng theo kiến trúc
event-driven.

## Trạng thái hiện tại

Project hiện chỉ có:

- Cấu trúc Maven multi-module.
- Cấu hình Kafka chạy KRaft, Kafka UI và PostgreSQL.
- Cấu hình topic, partition, retention và log compaction.
- Cấu hình nền cho 5 service Spring Boot.
- Các thư mục rỗng dành cho source code, test và migration.

Project **chưa có** class Java, REST API, producer, consumer, Kafka Streams,
database migration hoặc test nghiệp vụ. Các phần này sẽ được tự thực hành theo
từng giai đoạn.

## Yêu cầu

- Java 21
- Maven 3.9+
- Docker và Docker Compose

## Khởi động hạ tầng

```bash
cp .env.example .env
docker compose up -d
```

Sau khi các container healthy:

- Kafka bootstrap server: `localhost:9093`
- Kafka UI: <http://localhost:8088>
- PostgreSQL: `localhost:5433`

Kiểm tra cấu hình Maven mà không cần source code:

```bash
mvn validate
```

Dừng hạ tầng:

```bash
docker compose down
```

Xóa cả dữ liệu local và bắt đầu lại:

```bash
docker compose down -v
```

## Module

| Module | Port | Database | Vai trò dự kiến |
|---|---:|---|---|
| order-service | 8081 | order_db | Nhận và theo dõi đơn hàng |
| inventory-service | 8082 | inventory_db | Giữ và hoàn trả tồn kho |
| payment-service | 8083 | payment_db | Thanh toán giả lập |
| notification-service | 8084 | notification_db | Gửi thông báo giả lập |
| analytics-service | 8085 | analytics_db | Tổng hợp dữ liệu bằng Kafka Streams |
| shared-events | - | - | Event contract dùng chung |

Xem [docs/architecture.md](docs/architecture.md) và
[infrastructure/kafka/topics.md](infrastructure/kafka/topics.md) trước khi bắt
đầu viết code.

