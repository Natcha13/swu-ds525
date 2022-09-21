# Data Modeling I

## Getting Started

```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

### Prerequisite when install psycopg2 package

For Debian/Ubuntu users:

```sh
sudo apt install -y libpq-dev
```

For Mac users:

```sh
brew install postgresql
```

## Running Postgres

```sh
docker-compose up
```

connect postgres and login: http://localhost:8080/

```sh
 - System: PostgreSQL
 - Server: postgres
 - Username: postgres
 - Password: postgres
 - Database: postgres
```
create tables

```sh
$ python create_tables.py
```

insert data into tables:

```sh
$ python etl.py
```

To shutdown, press Ctrl+C and run:

```sh
docker-compose down
```

## deactivate the visual environment:
```sh
$ deactivate
```
