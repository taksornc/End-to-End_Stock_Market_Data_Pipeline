# 📈 End-to-End Stock Market Data Pipeline

Data Pipeline สำหรับดึงข้อมูลตลาดหุ้นจาก Yahoo Finance API, จัดเก็บข้อมูลดิบ (Raw Data) ลงใน Object Storage (MinIO), แปลงโครงสร้างข้อมูลด้วย PySpark บน Docker Container, โหลดข้อมูลเข้าสู่ PostgreSQL Data Warehouse ด้วย Astro SDK, นำข้อมูลไปวิเคราะห์และสร้าง Dashboard แสดงผลบน Metabase พร้อมระบบแจ้งเตือนผ่าน Slack API

---

## 🛠️ เทคโนโลยีที่ใช้ (Tech Stack & Tools)

- **Data Orchestration:** Apache Airflow (Astronomer CLI) & Astro SDK
- **Data Source:** Yahoo Finance API
- **Data Lake / Object Storage:** MinIO (S3-compatible)
- **Data Processing Engine:** Apache Spark (PySpark inside Docker Container)
- **Data Warehouse:** PostgreSQL
- **Data Visualization:** Metabase
- **Alerting System:** Slack API (`SlackNotifier`)
- **Infrastructure:** Docker & Docker Compose (`docker-compose.override.yml`)

---

## 🏗️ ลำดับการทำงานของ Pipeline (Workflow Architecture)

1. **`is_api_available` (Sensor):** ตรวจสอบความพร้อมการใช้งานของ Yahoo Finance API
2. **`get_stock_prices` (PythonOperator):** ดึงข้อมูลราคาหุ้นรายวัน (เช่น หุ้น NVIDIA - `NVDA`)
3. **`store_prices` (PythonOperator):** บันทึกข้อมูลดิบในรูปแบบ JSON ลงใน MinIO Object Storage
4. **`format_prices` (DockerOperator):** เรียกใช้ Docker Container เพื่อรัน PySpark Job (`airflow/stock-app`) สื่อสารผ่าน `docker-proxy` เพื่อทำ Cleaning และแปลงไฟล์เป็น CSV
5. **`get_formatted_csv` (PythonOperator):** ดึง S3 Path ของไฟล์ CSV ที่ประมวลผลแล้วใน MinIO
6. **`load_to_dw` (Astro SDK `load_file`):** โหลดข้อมูล CSV จาก MinIO เข้าสู่ตาราง `stock_market` บน PostgreSQL Data Warehouse
7. **Slack Alerting:** ใช้ `SlackNotifier` ของ Airflow (`on_success_callback` และ `on_failure_callback`) เพื่อส่งการแจ้งเตือนสถานะ Pipeline ไปยัง Slack Channel แบบ Real-time

---

## 📁 โครงสร้างโปรเจกต์ (Project Structure)

```text
.
├── dags/
│   └── stock_market.py         # ไฟล์กำหนดการทำงานหลักของ Airflow DAG
├── include/
│   └── stock_market/
│       ├── tasks.py            # ฟังก์ชัน Python สำหรับ Task ต่างๆ ใน Airflow
│       └── spark/              # Dockerfile และสคริปต์ PySpark Transformation
├── docker-compose.override.yml # คอนฟิกบริการ docker-proxy, spark-master, spark-worker
├── requirements.txt            # Python dependencies (Astro SDK, Slack provider ฯลฯ)
└── README.md
