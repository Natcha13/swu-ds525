# Building a Data Warehouse

## Data Model
![data model](pic/data model_dw.png)

### 1. Create AWS S3
![AWS S3](pic/S3.png)

### 2. Create AWS Redshift and enable public
![AWS Redshift](pic/Redshift.png)

### 3. S3 Connection path
![S3 Connection path](pic/Redshift Connect.png)

### 4. Redshift Connection path
![Redshift Connection path](pic/S3 Connect.png)


## Started
### Getting Started
```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

### Running ETL Script
```sh
python etl.py
```