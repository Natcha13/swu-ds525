# Capstone Project

## Data model: Data warehouse
![Data Model](pic/Data_Model.jpg)

## Project Documentation
[Documentation link](https://github.com/Natcha13/swu-ds525/blob/7ea38a1759cac4d25aeb7373135d392e6c8e5549/capstone-project/Instruction_Capstone_Project.pdf)

## Run command
```sh
cd capstone-project
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

## Prepare your AWS access (GET your credential on AWS terminal)
```sh
cat ~/.aws/credentials
```

## Upload data to S3
```sh
python etl_s3.py
```

## Create table and load data from S3 to Redshift
Running Airflow

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

```sh
docker-compose up
```

## Use dbt for data modeling and data tranforming
Create a dbt project

```sh
dbt init
```

Test dbt connection

```sh
cd dbt_empdata
dbt debug
```

You should see "All checks passed!".


## Dashboard Presentation
[Presentation link](https://github.com/Natcha13/swu-ds525/blob/e99baa9e31bd83a93f8fa96dbd752f2c0fca734d/capstone-project/Dashboard-MoviesOnStreamingPlatforms.pdf)

<br>
## And finally do not forget to shutdown
-  Stop services by shutdown Docker <br>
```sh
docker-compose down
```
<br>
- Deactivate the virtual environment <br>

```sh
$ deactivate
```