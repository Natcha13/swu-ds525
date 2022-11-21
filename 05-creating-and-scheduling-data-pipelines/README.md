# Creating and Scheduling Data Pipelines
## Data Diagram
(pic/DataModel1.png)


## Get start
### 1. change directory to project 04-building-a-data-lake:
```sh
cd 05-creating-and-scheduling-data-pipelines
```

### 2. prepare environment workspace
ถ้าใช้งานระบบที่เป็น Linux ให้เรารันคำสั่งด้านล่างนี้ก่อน

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

หลังจากนั้นให้รัน

```sh
docker-compose up
```

เราจะสามารถเข้าไปที่หน้า Airflow UI ได้ที่ port 8080

เสร็จแล้วให้คัดลอกโฟลเดอร์ `data` ที่เตรียมไว้ข้างนอกสุด เข้ามาใส่ในโฟลเดอร์ `dags` เพื่อที่ Airflow จะได้เห็นไฟล์ข้อมูลเหล่านี้ แล้วจึงค่อยทำโปรเจคต่อ

**หมายเหตุ:** จริง ๆ แล้วเราสามารถเอาโฟลเดอร์ `data` ไว้ที่ไหนก็ได้ที่ Airflow ที่เรารันเข้าถึงได้ แต่เพื่อความง่ายสำหรับโปรเจคนี้ เราจะนำเอาโฟลเดอร์ `data` ไว้ในโฟลเดอร์ `dags` เลย

### 3. config Airflow
 เข้า Airflow ได้ที่ port 8080 (localhost:8080)
 จากนั้นให้ไปที่ tab admin เลือก  connection
 และเพิ่ม connection
(pic/Picture1.png)


## data schedule
ให้เข้าที่ tab Dags แล้ว etl โดยเราสามารถตรวจสอบการทำงานได้ในหน้า Graph
(pic/Picture2.png)

ซึ่งถ้า schedule ทำงานปกติจะเป็นสีเขียว

### 4. config Airflow
เข้า Postgres ได้ที่ service Adminer ได้ที่ port 8088 (localhost:8088) 
เพื่อ check data data ว่าเข้าสู่ database เราหรือไม่ ให้เข้าที่ postgres
