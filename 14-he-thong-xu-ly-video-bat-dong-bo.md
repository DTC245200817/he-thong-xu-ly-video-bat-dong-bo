# Đề tài 14 - Hệ thống xử lý video bất đồng bộ

## 1. Mô tả đề tài

Xây dựng hệ thống tiếp nhận video và thực hiện một hoặc nhiều công việc xử lý nền, chẳng hạn tạo ảnh đại diện, lấy metadata hoặc giả lập chuyển đổi chất lượng. Công việc được phân phối cho Worker qua RabbitMQ.

Kiến trúc tối thiểu của hệ thống:

```text
Client -> Media API -> RabbitMQ -> Media Worker -> File Storage
             |                         |
             +------> Database <-------+
```

## 2. Chức năng chính

1. Đăng ký hoặc upload một video.
2. Tạo job xử lý video.
3. Đưa job vào queue.
4. Worker nhận và thực hiện tác vụ xử lý.
5. Cập nhật tiến trình hoặc trạng thái xử lý.
6. Lưu thông tin kết quả sau xử lý.
7. Tra cứu trạng thái job.
8. Tra cứu danh sách video đã xử lý.

## 3. API tối thiểu

```text
POST /videos
POST /videos/{id}/jobs
GET  /videos
GET  /videos/{id}
GET  /jobs/{id}
```

Dữ liệu tối thiểu gồm video, loại tác vụ, job xử lý, trạng thái và kết quả đầu ra.
