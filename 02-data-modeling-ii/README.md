# Data Modeling II


## Getting Started

##### directory to project 02-data-modeling-ii
```sh
$ cd 02-data-modeling-ii
```

##### Started
```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

## Running Cassandra

```sh
docker-compose up
```

## Create table ,Insert and select data
```sh
python etl.py
```

## Shutdown steps

##### stop Cassandra service by shutdown Docker:
```sh
$ docker-compose down
```

##### deactivate the visual environment:
```sh
$ deactivate
```
