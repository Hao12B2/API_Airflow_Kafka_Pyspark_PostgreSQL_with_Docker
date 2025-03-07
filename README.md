# MAIN GOAL OF PROJECT
<img src="/Architecture.jpg" width="1000" height="400">
<br /> The main goal of this project is to extract data from websites that provide Currency Exchange Rates for selected currencies, with the base currency being USD. 

<br /> In addition, the data is transformed and loaded into a database to display the current exchange rates for selected currencies.

### MANUAL INSTRUCTION
<br />To start, run **`docker-compose up -d`** in the terminal to create and start containers in Docker.
<br />DAGs need to be executed in an Apache Airflow web server.

<br />1. Open Airflow's web server at `localhost:8080` and log in with the username **admin** and password **admin**.

<br />2. In the Docker Kafka Container, open the terminal and run the following command: `kafka-topics --create --topic exchangerate --bootstrap-server kafka1:9092,kafka2:9093,kafka3:9094 --replication-factor 3 --partitions 3`

<br />3. Go back to the web server, click run DAG, and you will see the state of tasks.

<br />4. In the terminal of the Kafka container, run `kafka-console-consumer --topic exchangerate --bootstrap-server kafka1:9092,kafka2:9093,kafka3:9094 --from-beginning`. You will see the data loaded into it.


After that, we will load the data into PostgreSQL using PySpark.

<br />1. In the terminal, run `docker-compose exec postgres psql -U airflow -d airflow -c 'CREATE TABLE IF NOT EXISTS "exchangerate" ( date TIMESTAMP, base VARCHAR(255), USD FLOAT, EUR FLOAT, GBP FLOAT, SEK FLOAT, CHF FLOAT, INR FLOAT, CNY FLOAT, HKD FLOAT, JPY FLOAT, THB FLOAT, VND FLOAT );'`

<br />2. Then, run `spark-submit --master spark://spark-master:7077 --jars /opt/bitnami/spark/jars/kafka-postgresql/spark-sql-kafka-0-10_2.12-3.5.5.jar,/opt/bitnami/spark/jars/kafka-postgresql/kafka-clients-3.4.0.jar,/opt/bitnami/spark/jars/kafka-postgresql/postgresql-42.7.5.jar,/opt/bitnami/spark/jars/kafka-postgresql/commons-pool2-2.11.1.jar,/opt/bitnami/spark/jars/kafka-postgresql/spark-streaming-kafka-0-10_2.12-3.3.0.jar,/opt/bitnami/spark/jars/kafka-postgresql/spark-token-provider-kafka-0-10_2.12-3.3.0.jar jobs/python/load_exchangeRateToDW.py`

<br />3. Finally, run `docker-compose exec postgres psql -U airflow -d airflow -c 'SELECT * FROM exchangerate LIMIT 10;'` to check the result.


###### To create this project, these sites were used: [Apache Kafka](https://kafka.apache.org/), [Apache Airflow](https://airflow.apache.org/), [Kafka-Python](https://kafka-python.readthedocs.io/en/master/), [Docker Desktop](https://www.docker.com/products/docker-desktop/), [Apache Spark](https://spark.apache.org/), [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html), and [PostgreSQL](https://www.postgresql.org/).
