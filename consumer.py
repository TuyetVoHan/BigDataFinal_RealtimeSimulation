import json
from confluent_kafka import Consumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# 1. Cấu hình InfluxDB (Khớp với Docker)
token = "my-super-secret-token"
org = "my-org"
bucket = "github-metrics"
url = "http://localhost:8086"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# 2. Cấu hình Kafka Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'github-monitor-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['github-events'])

print("Đang xử lý dữ liệu và đẩy vào InfluxDB...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
            
        event_data = json.loads(msg.value().decode('utf-8'))
        event_type = event_data.get('type', 'UnknownEvent')
        
        # 3. Tạo một điểm dữ liệu (Point) để gửi vào InfluxDB
        # Bỏ đi đuôi .time() bị lỗi
        point = Point("github_events_metric") \
            .tag("event_type", event_type) \
            .field("count", 1)

        write_api.write(bucket=bucket, org=org, record=point)
        print(f"Đã ghi nhận: {event_type}")

except KeyboardInterrupt:
    print("Dừng hệ thống.")
finally:
    consumer.close()
    client.close()