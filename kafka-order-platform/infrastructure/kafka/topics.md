# Topic catalog

Tất cả topic được tạo bởi service `kafka-init` trong `compose.yaml`.

| Topic | Partition | Cleanup | Retention | Message key dự kiến |
|---|---:|---|---|---|
| order.created.v1 | 3 | delete | 7 ngày | orderId |
| inventory.reserved.v1 | 3 | delete | 7 ngày | orderId |
| inventory.rejected.v1 | 3 | delete | 7 ngày | orderId |
| payment.completed.v1 | 3 | delete | 7 ngày | orderId |
| payment.failed.v1 | 3 | delete | 7 ngày | orderId |
| order.completed.v1 | 3 | delete | 7 ngày | orderId |
| order.cancelled.v1 | 3 | delete | 7 ngày | orderId |
| notification.requested.v1 | 3 | delete | 7 ngày | orderId |
| notification.sent.v1 | 3 | delete | 7 ngày | orderId |
| order.events.retry.v1 | 3 | delete | 3 ngày | orderId |
| order.events.dlq.v1 | 3 | delete | 30 ngày | orderId |
| order.state.v1 | 3 | compact | không giới hạn theo thời gian | orderId |
| order.metrics.v1 | 3 | compact,delete | 30 ngày | metric key |

Replication factor bằng 1 vì môi trường local chỉ có một broker. Đây không phải
cấu hình dành cho production.

