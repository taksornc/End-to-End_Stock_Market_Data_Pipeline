from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from minio import Minio
from io import BytesIO
import json
import requests

# -----------------------------------------------------------------------------
# Global Variable: กำหนดชื่อ Bucket กลางสำหรับใช้งานในทุกฟังก์ชัน
# -----------------------------------------------------------------------------
BUCKET_NAME = 'stock-market'

# -----------------------------------------------------------------------------
# Helper Function: สร้างและคืนค่า MinIO Client (ลดการเขียนโค้ดซ้ำ)
# -----------------------------------------------------------------------------
def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    return Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )

# -----------------------------------------------------------------------------
# Task 1: ดึงราคาหุ้นจาก API
# -----------------------------------------------------------------------------
def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

# -----------------------------------------------------------------------------
# Task 2: จัดเก็บ JSON ลง MinIO (ปรับมาใช้ _get_minio_client และ BUCKET_NAME)
# -----------------------------------------------------------------------------
def _store_prices(stock):
    client = _get_minio_client()
    
    # สร้าง Bucket หากยังไม่มี
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'

# -----------------------------------------------------------------------------
# Task 3: ดึง Path ของไฟล์ CSV ที่จัดรูปแบบแล้วจาก MinIO
# -----------------------------------------------------------------------------
def _get_formatted_csv(path):
    client = _get_minio_client()
    
    # แยกเอาเฉพาะชื่อโฟลเดอร์สัญลักษณ์หุ้น (เช่น จาก 'stock-market/NVDA' นำมาเฉพาะ 'NVDA')
    # เพื่อประกอบเป็น Prefix: 'NVDA/formatted_prices/'
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    
    # ค้นหารายการไฟล์ทั้งหมดภายใต้ Prefix ใน Bucket
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    
    # วนลูปตรวจหาไฟล์ที่ลงท้ายด้วย .csv
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name  # คืนค่า Path ไฟล์ CSV (เช่น NVDA/formatted_prices/part-xxx.csv)
            
    # หากค้นหาไม่พบไฟล์ CSV ให้โยน Exception แจ้งเตือน Airflow
    raise AirflowNotFoundException('The CSV file does not exist')