# Personal Finances

## Pre-requirements:
Some tools are required in order to run the project locally:

* Docker + Docker Compose
* RPK (RedPanda CLI)
* Flink CLI

## Running Locally

To run it locally:

```bash
cd services/infra
make run

cd services/infra
make create-topics

cd services/processing_flink
./job.sh run postgresql_sink

cd services/processing_flink
./job.sh run processing
```

Import the frontend panel on Appsmith. To do this, import the `services/infra/dashboard.json` file. A tutorial on how to do this can be [found here](https://www.youtube.com/watch?v=1YpIzX4DF28). When importing the dashboard, you'll be ask for database connection settings, which are the following:

| Config | Value |
| -- | -- |
| Host | `postgres` |
| Port | `5432` |
| User | `appsmith` |
| Password | `appsmith` |
