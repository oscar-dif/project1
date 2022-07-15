

-------------------------------------------------------------------------------
For sql in ubuntu:

from sqlalchemy import *
import psycopg2
import credentials

 

DB=credentials.database
USER=credentials.user
PASS=credentials.password

 

conn_string = f'postgresql://{USER}:{PASS}@127.0.0.1/{DB}'
db = create_engine(conn_string)
conn1 = db.connect()

 

conn2 = psycopg2.connect(
   database=DB,
   user=USER, 
   password=PASS,
   host='127.0.0.1', 
   port='5432' # ska vara 5433 i WSL Ubuntu
)

 

# test sqlalchemy:
result = db.execute(text("select * from wdata;"))
for r in result:
    print(r)
result = db.execute(text("select d from wdata;"))
for r in result:
    print(r)

 

# test psycopg2:

 

C2 = conn2.cursor()
C2.execute('select * from wdata;')
L = C2.fetchall()
C2.close()
print(L)


-------------------------------------------------------------------------------

 install on ubuntu:

    check if exists in python:
        (valt program).__version__

    pandas:
    sudo apt install python3-pandas
    in venv:
        pip install pandas

    json:
    built in

    psql:
    sudo apt-get install postgresql
    in venv:
        pip install psql
    
    requests:
    in venv:
        pip install requests

    alchemy:
    sudo apt-get -y install python-sqlalchemy
    in venv:
        pip install alchemy

    pip:
    sudo apt install python3-pip

    matplotlib:
    in venv:
        pip install matplotlib

    psycopg2:
    in venv:
        pip install psycopg2



update ubuntu by:
sudo apt update && sudo apt upgrade -y

-------------------------------------------------------------------------------

active postgres in venv ubuntu:
    start venv:
        #!/bin/bash
    . ./venv/bin/activate
    export AIRFLOW__API__AUTH_BACKEND='airflow.api.auth.backend.basic_auth'
    airflow standalone

    start postgres:
    sudo service postgresql start
    sudo -u postgres psql

    create postgres user:
    create user danieljs with password 'abc123';

    create database in postgres ubuntu:
    create database weather_data

    grant privileges on database:
    grant all privileges on database weather_data to danieljs;

    change database owner:
    alter database weather_data owner to danieljs;

    open psql and database:
    psql weather_data



    