# Train-Ticket Dataset - Cấu Trúc Dữ Liệu

Dataset train-ticket là tập hợp dữ liệu từ hệ thống microservice đặt vé tàu hỏa, thu thập từ tháng 7/2022. Dữ liệu bao gồm logs, traces phân tán, metrics giám sát, và danh sách các sự kiện bất thường.

## 1. Cấu Trúc Tổng Quan

```
data/raw/train-ticket/
├── case_01_admin_basic_info_spring_1_5_22/
├── case_02_auth_mongo_4_4_15_20220713/
├── case_03_auth_mongo_5_0_9_20220706/
├── case_04_auth_mongodb_4_4_15_20220727/
├── case_05_order_springboot_2_7_1_20220711/
├── case_06_order_mongodb_driver_3_0_4_20220713/
├── case_07_order_mongodb_4_2_2_20220712/
├── case_08_order_mongodb_4_4_15_20220712/
├── case_09_order_springdata_mongodb_1_5_22_20220711/
└── case_10_order_springdata_mongodb_2_0_0_20220711/
```

## 2. Cấu Trúc Mỗi Case

Mỗi case chứa ba loại dữ liệu chính:

```
case_XX_*/
├── LOGS_<service_name>.txt                           # Logs thô
├── LOGS_<service_name>.txt_structured.csv            # Logs cấu trúc hóa
├── LOGS_<service_name>.txt_templates.csv             # Templates log messages
├── Monitoring_<service_name>.json_YYYY-MM-DD/        # Metrics giám sát
│   ├── <service>_container_cpu_usage_seconds_total.json
│   ├── <service>_container_memory_working_set_bytes.json
│   ├── <service>_container_network_transmit_packets_total.json
│   ├── <service>_node_cpu_seconds_total.json
│   ├── <service>_node_memory_MemAvailable_bytes.json
│   ├── <service>_node_memory_MemTotal_bytes.json
│   ├── <service>_node_namespace_pod_container_container_cpu_usage_seconds_total_sum_irate.json
│   ├── <service>_node_namespace_pod_container_container_memory_working_set_bytes.json
│   ├── <service>_node_network_transmit_packets_total.json
│   ├── <service>_monitoring_container_memory_working_set_bytes_sum.json
│   └── <service>_memory_MemAvailable_bytes_sum.json
├── potentialAnomalies_<service_name>.txt             # Danh sách anomalies
└── Traces_<service_name>_YYYY-MM-DD/                 # Distributed traces
    ├── <service>_ts-admin-basic-info-service.json
    ├── <service>_ts-auth-service.json
    ├── <service>_ts-order-service.json
    ├── <service>_ts-payment-service.json
    └── ... (40+ services khác)
```

## 3. Chi Tiết 10 Cases

| #       | Service                  | Framework/Tech             | Ngày       |
| ------- | ------------------------ | -------------------------- | ---------- |
| Case 01 | Admin Basic Info Service | Spring Web 1.5.22          | 2022-07-08 |
| Case 02 | Auth Service             | MongoDB 4.4.15             | 2022-07-13 |
| Case 03 | Auth Service             | MongoDB 5.0.9              | 2022-07-06 |
| Case 04 | Auth Service             | MongoDB 4.4.15             | 2022-07-27 |
| Case 05 | Order Service            | Spring Boot 2.7.1          | 2022-07-11 |
| Case 06 | Order Service            | MongoDB Driver 3.0.4       | 2022-07-13 |
| Case 07 | Order Service            | MongoDB 4.2.2              | 2022-07-12 |
| Case 08 | Order Service            | MongoDB 4.4.15             | 2022-07-12 |
| Case 09 | Order Service            | Spring Data MongoDB 1.5.22 | 2022-07-11 |
| Case 10 | Order Service            | Spring Data MongoDB 2.0.0  | 2022-07-11 |

## 4. Logs - Raw Text Format

**Tệp**: `LOGS_<service_name>.txt`

**Ví dụ dữ liệu thô**:

```
2022-07-08 16:02:46.267  INFO 1 --- [io-16112-exec-4] i.j.internal.reporters.LoggingReporter   : Span reported: 139b0298ae4feb3a:139b0298ae4feb3a:0:1 - addOrder
2022-07-08 16:02:46.255  INFO 1 --- [io-12032-exec-2] i.j.internal.reporters.LoggingReporter   : Span reported: 139b0298ae4feb3a:d4e114d3c9e16e76:b638f8b526a57c2:1 - addcreateNewOrder
2022-07-08 16:02:46.255  INFO 1 --- [io-16112-exec-4] i.j.internal.reporters.LoggingReporter   : Span reported: 139b0298ae4feb3a:b638f8b526a57c2:139b0298ae4feb3a:1 - POST
2022-07-08 16:02:46.162  INFO 1 --- [io-12032-exec-2] other.service.OrderOtherServiceImpl      : [Order Service][Admin Add Order] Success.
2022-07-08 16:02:46.162  INFO 1 --- [io-12032-exec-2] other.service.OrderOtherServiceImpl      : [Order Service][Admin Add Order] Price: 123
2022-07-08 16:02:46.087  INFO 1 --- [io-12032-exec-2] other.service.OrderOtherServiceImpl      : [Order Service][Admin Add Order] Ready Add Order.
2022-07-08 16:02:46.075  INFO 1 --- [io-16112-exec-4] a.service.AdminOrderServiceImpl          : [Admin Order Service][Add New Order Other]
2022-07-08 16:02:14.653  INFO 1 --- [io-12340-exec-1] i.j.internal.reporters.LoggingReporter   : Span reported: 6560547e66ce2aaf:6560547e66ce2aaf:0:1 - getToken
2022-07-08 16:02:14.569  INFO 1 --- [io-12340-exec-1] auth.service.impl.TokenServiceImpl       : eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsInJvbGVzIjpbIlJPTEVfQURNSU4iXSwiaWQiOiI5ZjQzMTEwNS1iZGQ4LTRhY2QtOTYyYi1iODQ0ZDYxMGQ1MTMiLCJpYXQiOjE2NTcyNjczMzQsImV4cCI6MTY1NzI3MDkzNH0.EEAf5tYnauUIiQJDDzhMz9CT8VbyX7gaVgLxhPOF7aIUSER TOKEN
2022-07-08 16:02:14.569  INFO 1 --- [io-12340-exec-1] auth.service.impl.TokenServiceImpl       : 9f431105-bdd8-4acd-962b-b844d610d513   USER ID
2022-07-08 16:02:14.063  INFO 1 --- [io-12340-exec-1] auth.security.UserDetailsServiceImpl     : UsernamePasswordAuthenticationToken  username :admin
2022-07-08 16:02:14.062  INFO 1 --- [io-15678-exec-5] i.j.internal.reporters.LoggingReporter   : Span reported: 6560547e66ce2aaf:2b27b6928d9db106:8d7085af2248030a:1 - verifyCode
2022-07-08 16:02:14.060  INFO 1 --- [io-15678-exec-5] v.service.impl.VerifyCodeServiceImpl     : GET Code By cookieId A77EA06FAD274547A9CE0A0A91270BDF   is :MXDA
2022-07-08 16:02:06.962  INFO 1 --- [io-15678-exec-8] i.j.internal.reporters.LoggingReporter   : Span reported: aa71b007adc42867:aa71b007adc42867:0:1 - imageCode
2022-07-08 16:02:05.753  INFO 1 --- [io-12340-exec-9] i.j.internal.reporters.LoggingReporter   : Span reported: 9e29827745bc64d3:9e29827745bc64d3:0:1 - getToken
```

**Cấu trúc mỗi dòng**:

- **Timestamp**: `2022-07-08 16:02:46.267` - Thời gian (millisecond)
- **Level**: `INFO` - Mức độ (INFO, ERROR, WARN, DEBUG)
- **Thread**: `1 --- [io-16112-exec-4]` - ID và tên thread
- **Logger**: `i.j.internal.reporters.LoggingReporter` - Tên logger class
- **Message**: Nội dung log message

## 5. Logs - Structured CSV Format

**Tệp**: `LOGS_<service_name>.txt_structured.csv`

**Header**:

```
LineId,Date,Time,Level,Number,LoggingReporter,Content,EventId,EventTemplate,ParameterList
```

**Ví dụ dữ liệu**:

```csv
1,2022-07-08,16:02:46.267,INFO,1,[io-16112-exec-4]...,Span reported: 139b0298ae4feb3a:139b0298ae4feb3a:0:1 - addOrder,46c6d92e,Span reported: <*> - <*>,"['139b0298ae4feb3a:139b0298ae4feb3a:0:1', 'addOrder']"
2,2022-07-08,16:02:46.255,INFO,1,[io-12032-exec-2]...,Span reported: 139b0298ae4feb3a:d4e114d3c9e16e76:b638f8b526a57c2:1 - addcreateNewOrder,46c6d92e,Span reported: <*> - <*>,"['139b0298ae4feb3a:d4e114d3c9e16e76:b638f8b526a57c2:1', 'addcreateNewOrder']"
3,2022-07-08,16:02:46.255,INFO,1,[io-16112-exec-4]...,Span reported: 139b0298ae4feb3a:b638f8b526a57c2:139b0298ae4feb3a:1 - POST,46c6d92e,Span reported: <*> - <*>,"['139b0298ae4feb3a:b638f8b526a57c2:139b0298ae4feb3a:1', 'POST']"
4,2022-07-08,16:02:46.162,INFO,1,[io-12032-exec-2]...[Order Service][Admin Add Order] Success.,1ce21860,[Order Service][Admin Add Order] Success.,[]
5,2022-07-08,16:02:46.162,INFO,1,[io-12032-exec-2]...[Order Service][Admin Add Order] Price: 123,808bf2ef,[Order Service][Admin Add Order] Price: 123,[]
6,2022-07-08,16:02:46.087,INFO,1,[io-12032-exec-2]...[Order Service][Admin Add Order] Ready Add Order.,e0862f6c,[Order Service][Admin Add Order] <*> <*> <*>,"['Ready', 'Add Order.']"
7,2022-07-08,16:02:46.075,INFO,1,[io-16112-exec-4]...[Admin Order Service][Add New Order Other],d6e829a6,[Admin Order Service][Add New Order Other],[]
8,2022-07-08,16:02:14.653,INFO,1,[io-12340-exec-1]...Span reported: 6560547e66ce2aaf:6560547e66ce2aaf:0:1 - getToken,46c6d92e,Span reported: <*> - <*>,"['6560547e66ce2aaf:6560547e66ce2aaf:0:1', 'getToken']"
```

**Mô tả các cột**:

- **LineId**: ID dòng (1, 2, 3, ...)
- **Date**: Ngày (YYYY-MM-DD)
- **Time**: Giờ (HH:MM:SS.mmm)
- **Level**: Mức độ log
- **Number**: Thread ID
- **LoggingReporter**: Logger class
- **Content**: Nội dung đầy đủ
- **EventId**: Hash ID duy nhất của event template
- **EventTemplate**: Mẫu message với `<*>` thay cho tham số
- **ParameterList**: Danh sách tham số (JSON array)

## 6. Log Templates

**Tệp**: `LOGS_<service_name>.txt_templates.csv`

Chứa các template duy nhất được trích xuất từ logs, giúp nhóm các messages tương tự.

**Ví dụ**:

```
EventId,EventTemplate,Frequency
46c6d92e,Span reported: <*> - <*>,245
1ce21860,[Order Service][Admin Add Order] Success.,12
e0862f6c,[Order Service][Admin Add Order] <*> <*> <*>,8
d6e829a6,[Admin Order Service][Add New Order Other],5
808bf2ef,[Order Service][Admin Add Order] Price: <*>,6
```

## 7. Distributed Traces

**Thư mục**: `Traces_<service_name>_YYYY-MM-DD/`

**Ví dụ file**: `ts-admin-basic-info-service_ts-order-service.json`

**Cấu trúc JSON** (Jaeger format):

```json
{
  "traceID": "139b0298ae4feb3a",
  "spans": [
    {
      "traceID": "139b0298ae4feb3a",
      "spanID": "139b0298ae4feb3a",
      "operationName": "addOrder",
      "references": [],
      "startTime": 1657275766267000,
      "duration": 50000,
      "tags": {
        "span.kind": "client",
        "http.method": "POST",
        "http.url": "/order/add",
        "component": "spring-webmvc"
      },
      "logs": [
        {
          "timestamp": 1657275766275000,
          "fields": [{ "key": "event", "value": "order.started" }]
        }
      ],
      "processID": "p1",
      "warnings": null
    },
    {
      "traceID": "139b0298ae4feb3a",
      "spanID": "b638f8b526a57c2",
      "parentSpanID": "139b0298ae4feb3a",
      "operationName": "createNewOrder",
      "references": [
        {
          "refType": "CHILD_OF",
          "traceID": "139b0298ae4feb3a",
          "spanID": "139b0298ae4feb3a"
        }
      ],
      "startTime": 1657275766270000,
      "duration": 45000,
      "tags": {
        "span.kind": "internal",
        "service.name": "ts-order-service",
        "db.type": "mongodb"
      },
      "processID": "p2",
      "warnings": null
    }
  ],
  "processes": {
    "p1": { "serviceName": "ts-admin-basic-info-service", "tags": [] },
    "p2": { "serviceName": "ts-order-service", "tags": [] }
  }
}
```

**Cấu trúc một span**:

- **traceID**: ID trace (dùng để nhóm tất cả spans của một request)
- **spanID**: ID của span này
- **parentSpanID**: ID của span cha
- **operationName**: Tên hoạt động (GET, POST, createNewOrder, etc.)
- **startTime**: Thời gian bắt đầu (microseconds)
- **duration**: Thời gian thực thi (microseconds)
- **tags**: Thông tin metadata (HTTP method, service name, etc.)
- **logs**: Các event được log trong quá trình thực thi
- **processID**: Reference đến process (service) phát sinh span

**Services trong hệ thống** (từ files traces):

```
ts-admin-basic-info-service    ts-order-service          ts-station-service
ts-admin-order-service         ts-order-other-service    ts-ticket-office-service
ts-admin-route-service         ts-payment-service        ts-ticketinfo-service
ts-admin-travel-service        ts-preserve-other-service ts-train-service
ts-admin-user-service          ts-preserve-service       ts-travel-plan-service
ts-assurance-service           ts-price-service          ts-travel-service
ts-auth-service                ts-rebook-service         ts-travel2-service
ts-basic-service               ts-route-plan-service     ts-user-service
ts-cancel-service              ts-route-service          ts-verification-code-service
ts-config-service              ts-seat-service           ts-voucher-service
ts-consign-price-service       ts-security-service       (40+ services)
ts-consign-service
ts-contacts-service
ts-execute-service
ts-food-map-service
ts-food-service
ts-inside-payment-service
ts-news-service
ts-notification-service
```

## 8. Metrics - Giám Sát Tài Nguyên

**Thư mục**: `Monitoring_<service_name>.json_YYYY-MM-DD/`

**Ví dụ file**: `ts-admin-basic-info-service_springstarterweb_1.5.22.RELEASE.json_container_cpu_usage_seconds_total.json`

**Cấu trúc dữ liệu**:

```json
[
  {
    "metric": {
      "__name__": "container_cpu_usage_seconds_total",
      "pod_name": "ts-admin-basic-info-service-1",
      "container_name": "ts-admin-basic-info-service",
      "namespace": "default",
      "node": "node-1"
    },
    "values": [
      [1657275600, "1234.5"],
      [1657275660, "1234.8"],
      [1657275720, "1235.1"],
      [1657275780, "1235.4"],
      [1657275840, "1235.7"],
      [1657275900, "1236.0"]
    ]
  }
]
```

**Các loại metrics**:

- `container_cpu_usage_seconds_total.json` - CPU sử dụng (giây)
- `container_memory_working_set_bytes.json` - Memory sử dụng (bytes)
- `container_network_transmit_packets_total.json` - Network packets truyền
- `node_cpu_seconds_total.json` - CPU tổng của node
- `node_memory_MemAvailable_bytes.json` - Memory sẵn có trên node
- `node_memory_MemTotal_bytes.json` - Memory tổng trên node
- `node_namespace_pod_container_container_cpu_usage_seconds_total_sum_irate.json` - Tốc độ CPU (instant rate)
- `node_namespace_pod_container_container_memory_working_set_bytes.json` - Tổng memory pods

**Cấu trúc mỗi data point**:

- **[timestamp, value]**: timestamp (Unix seconds), giá trị metric

## 9. Potential Anomalies

**Tệp**: `potentialAnomalies_<service_name>.txt`

**Ví dụ nội dung**:

```
Timeout Server - no response Ticket-Reserve-QueryLeftTicket:

2022-07-08 13:49:15.159  INFO 1 --- [io-12346-exec-6] travel.service.TravelServiceImpl         : Query for Station id is: Response(status=1, msg=Success, data=shanghai)
2022-07-08 13:49:16.266  INFO 1 --- [io-12346-exec-6] i.j.internal.reporters.LoggingReporter   : Span reported: 7754a46b9d12b8cb:82e727fa0dc9c8af:7754a46b9d12b8cb:1 - GET
2022-07-08 13:49:16.271  INFO 1 --- [io-12346-exec-6] travel.service.TravelServiceImpl         : Query for Station id is: Response(status=1, msg=Success, data=suzhou)
2022-07-08 13:49:16.458  INFO 1 --- [io-12346-exec-6] travel.service.TravelServiceImpl         : [Travel Service][Get Route By Id] Route ID：92708982-77af-4318-be25-57ccb0ff69ad
2022-07-08 13:49:20.457  INFO 1 --- [io-12346-exec-3] travel.controller.TravelController       : [Travel Service] Query TripResponse
2022-07-08 13:49:20.767  INFO 1 --- [io-12346-exec-3] i.j.internal.reporters.LoggingReporter   : Span reported: 945a7008083601ac:3f619e8824537d0e:945a7008083601ac:1 - GET


noch ienmal probiert: viel schneller
2022-07-08 13:57:09.655  INFO 1 --- [io-12346-exec-2] i.j.internal.reporters.LoggingReporter   : Span reported: 8ca38c3ef67fecfb:953a8503acb0ce86:c1570c88185ac73c:1 - GET
2022-07-08 13:57:09.661  INFO 1 --- [io-12346-exec-2] i.j.internal.reporters.LoggingReporter   : Span reported: 8ca38c3ef67fecfb:c1570c88185ac73c:a54f14bf7d6586e5:1 - getTrainTypeByTripId
2022-07-08 13:57:09.767  INFO 1 --- [io-12346-exec-9] i.j.internal.reporters.LoggingReporter   : Span reported: 8ca38c3ef67fecfb:bf8faca7d9c3c707:8ca38c3ef67fecfb:1 - POST
2022-07-08 13:57:09.768  INFO 1 --- [io-12346-exec-9] travel.service.TravelServiceImpl         : Get Rest tickets num is: Response(status=1, msg=Get Left Ticket of Internal Success, data=1073741823)


Timeout server preserve ticket service --> Gateway Timeout (Server did not respond in Time):
2022-07-08 13:59:04.953  INFO 1 --- [io-14568-exec-8] preserve.controller.PreserveController   : [Preserve Service][Preserve] Account  order from Shang Hai -----> Su Zhou at Sat Dec 24 08:00:00 CST 2022
2022-07-08 13:59:04.954  INFO 1 --- [io-14568-exec-8] preserve.service.PreserveServiceImpl     : [Preserve Service] [Step 1] Check Security
2022-07-08 13:59:04.954  INFO 1 --- [io-14568-exec-8] preserve.service.PreserveServiceImpl     : [Preserve Other Service][Check Security] Checking....
2022-07-08 13:59:29.471  INFO 1 --- [io-14568-exec-8] i.j.internal.reporters.LoggingReporter   : Span reported: 839fbd7fa5eec4ac:5c79abf2fa7b6f9f:839fbd7fa5eec4ac:1 - GET
2022-07-08 13:59:29.766  INFO 1 --- [io-14568-exec-8] preserve.service.PreserveServiceImpl     : [Preserve Service] [Step 1] Check Security Complete
```

**Loại anomalies**:

- **Timeout**: Server không phản hồi (13:49:15 đến 13:49:20 = 5 giây timeout)
- **Slow Response**: So sánh thời gian: 5 giây vs 1 giây bình thường
- **Gateway Timeout**: 13:59:04 đến 13:59:29 = 25 giây timeout
