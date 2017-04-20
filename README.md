# HTAPBench

The Hybrid Transactional and Analytical Processing Benchmark is targeted at assessing engines capable of delivering mixed workloads composed of OLTP transactions and OLAP business queries without resorting to ETL.

There are a few requirements to run HTAPBench:
1. You need to have JAVA distribution (> 1.7) installed on the machine running HTAPBench.
2. You need to have installed the JDBC driver for the database you want to test.

# A. Build HTAPBench:
```bash
	mvn clean compile package
```

# B. Configure HTAPBench:
Clone and adjust the configuration file in config/htapb_config_postgres.xml to you test case.

Before you continue ensure that:
- The database engine you wish to test is installed and that you reach it from the machine running HTAPBench.
- In the database engine to be tested, create a test database e.g., htapb.
- In the database engine to be tested, create a user/password and grant all privileges to your test database.
- In the database engine to be tested, install the database schema.
```bash
java -cp .:target/htapbench-0.95-jar-with-dependencies.jar pt.haslab.htapbench.core.HTAPBench -b htapb -c your_config_file.xml --create true --load false --generateFiles false --filePath dir --execute false --calibrate false
```
# C. Populate
Before running HTAPBench, you will need to load data into the database. You have 2 choices:

1. Generate the CSV files to populate the database. (We recommend this method as it usually loads data faster.)
```bash
java -cp .:target/htapbench-0.95-jar-with-dependencies.jar pt.haslab.htapbench.core.HTAPBench -b htapb -c your_config_file.xml --generateFiles true --filePath dir --execute false --calibrate true
```
Afterwards you need to connect to the database engine console and use a Bulk Load command.
e.g., in Postgresql use the psql command to establish a connection and load each table in the schema.
```bash
> psql -h Postgres_host_IP -p Postgres_host_port -U postgres_user -d database_name
> COPY WAREHOUSE FROM '/dir/warehouse.csv' USING DELIMITERS ',';
```

2. Populate the database directly from HTAPBench. This internally establishes a connection and performs insert statements.
```bash
java -cp .:target/htapbench-0.95-jar-with-dependencies.jar pt.haslab.htapbench.core.HTAPBench -b htapb -c your_config_file.xml --load true --execute false --calibrate true
```






