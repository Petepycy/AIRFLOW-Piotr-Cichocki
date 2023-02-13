# Airflow Capstone Project
* [Introduction](#introduction)
* [Deployment](#deployment)
* [Technologies](#technologies)
* [Setup](#setup)
* [DAG Details](#dag-details)

## Introduction
The project consist of two Apache Airflow DAGs which implement trigger to generate table update. 
Trigger DAG will wait for file ‘run’ in some folder to appear and then trigger the table updating DAG.
The following DAGs are implemented:
* jobs_dag
* trigger_dag

## Deployment

<h4>Environment Variables & Airflow Variables & Airflow Connections</h4>
Environment variables are specified in .env file.
Add airflow variables manually in the Airflow UI. List of the required variables:
* Key : run, Value : /path to folder/

Airflow connection:
* Define fs_default 
* Define postgres_local

## Technologies
Project is created with:
* Python 3.8
* Apache Airflow 2.5
* Hashicorp Vault 1.12.2
* PostgreSQL 13.1
* Redis 5.0.5
* Docker

## Setup
To run this project you have to:
* Install Docker Community Edition (CE) on your workstation.Depending on your OS, you may need to configure Docker to use at least 4.00 GB of memory for the Airflow containers to run properly.
* Install Docker Compose v1.29.1 or newer on your workstation.
* To deploy Airflow on Docker Compose, you should fetch docker-compose.yaml.

```
curl -LfO  'https://github.com/gridu/AIRFLOW-Piotr-Cichocki/blob/main/docker-compose.yaml'
```

Go to the workspace and open terminal. Now you can start all services by running:

```
docker-compose up
```

# Dag Details
Jobs_dag dynamically generates three dags that consist of:
* PythonOperator which passes value to the log.
* BashOperator which get the current user by passing whoami commend.
* BranchPythonOperator which decide if there already is table in database.
* PostgresOperator which create table and insert row in.
* PostgreSQLCountRowsOperator which is custom operator that counts the number of rows in database.

Trigger_dag consist of: 
* SmartFileSensor which improve the efficiency of long running tasks
by using centralized processes to execute tasks in batches. To use it you need to serialize the task information into the database.
It uses custom-made class that inherit from FileSensor class.
* TriggerDagRunOperator which launches another dag that is specified in trigger_dag_id.
* TaskGroup which consist of:
* * ExternalTaskSensor which wait for another dag run completion.
* * PythonFunctionalOperator which takes XCOM value from jobs_dag task.
* * BashOperator which remove and create file on completion in the workspace folder.
* SlackAPIPostOperator which send message after completion of dag run and uses hashicorp vault storing slack token.

