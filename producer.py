import json
import gzip
import time
from confluent_kafka import Producer

# 1. Cấu hình kết nối tới Kafka Broker đang chạy trên Docker
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic_name = 'github-events'

# Hàm callback để kiểm tra xem tin nhắn có gửi thành công không
def delivery_report(err, msg):
    if err is not None:
        print(f"Lỗi gửi tin nhắn: {err}")

# 2. Đường dẫn tới file dữ liệu
file_path = 'data/2026-04-16-12.json.gz'

def simulate_streaming():
    print("Bắt đầu giả lập luồng dữ liệu thời gian thực...")
    count = 0
    chunk_size = 50 # Gửi 50 sự kiện một lần (Chunking)

    try:
        # Đọc trực tiếp file .gz
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                # Phân tích chuỗi JSON
                event = json.loads(line)
                
                # Lấy ra một số thông tin cơ bản để log ra màn hình
                event_type = event.get('type')
                actor_login = event.get('actor', {}).get('login')
                
                # Chuyển object event ngược lại thành chuỗi JSON dạng byte để gửi
                producer.produce(
                    topic_name,
                    value=json.dumps(event).encode('utf-8'),
                    callback=delivery_report
                )
                
                # Yêu cầu Kafka gửi dữ liệu đi ngay
                producer.poll(0)
                count += 1

                # 3. Logic giả lập streaming: Tạm dừng sau mỗi chunk
                if count % chunk_size == 0:
                    producer.flush() # Đảm bảo toàn bộ chunk đã được đẩy vào Kafka
                    print(f"Đã đẩy thành công {count} events. Đang chờ 1 giây...")
                    time.sleep(1) # Tạo độ trễ 1 giây
                    
    except KeyboardInterrupt:
        print("\nĐã dừng tiến trình giả lập bằng phím tắt.")
    except Exception as e:
        print(f"\nCó lỗi xảy ra: {e}")
    finally:
        # Chờ các tin nhắn cuối cùng được gửi đi trước khi đóng
        producer.flush()
        print("Đã đóng kết nối Kafka Producer.")

if __name__ == '__main__':
    simulate_streaming()