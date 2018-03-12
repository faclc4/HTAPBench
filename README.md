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
Clone and adjust the example configuration file in config/htapb_config_postgres.xml to you test case.

Before you continue ensure that:
- The database engine you wish to test is installed and that you can reach it from the machine running HTAPBench.
- That the database engine to be tested is configured with the required memory and that the max_clients allowance is enough for you setup.
- In the database engine to be tested, create a test database e.g., htapb.
- In the database engine to be tested, create a user/password and grant all privileges to your test database.
- In the database engine to be tested, install the database schema.
```bash
java -cp .:target/htapbench-0.95-jar-with-dependencies.jar pt.haslab.htapbench.core.HTAPBench -b database_name -c your_config_file.xml --create true --load false --generateFiles false --filePath dir --execute false --calibrate false
```
# C. Populate
Before running HTAPBench, you will need to load data into the database. The generated workload is computed according to the configured TPS. If you change this parameter, you need to generate the database files again. 

You have 2 choices

1. Generate the CSV files to populate the database. (We recommend this method as it usually loads data faster.)
```bash
java -cp .:target/htapbench-0.95-jar-with-dependencies.jar pt.haslab.htapbench.core.HTAPBench -b database_name -c your_config_file.xml --generateFiles true --filePath dir --execute false --calibrate true
```
Afterwards you need to connect to the database engine console and use a Bulk Load command.
e.g., in Postgresql use the psql command to establish a connection and load each (e.g., WAREHOUSE and OORDER) table in the schema. 
```bash
> psql -h Postgres_host_IP -p Postgres_host_port -U postgres_user -d database_name
> COPY WAREHOUSE FROM '/dirwarehouse.csv' USING DELIMITERS ',';
> COPY OORDER FROM '/dirorder.csv' USING DELIMITERS ',' WITH NULL as 'null';
```
(different database engines will have different commands. Check the respective documentation)

2. Populate the database directly from HTAPBench. This internally establishes a connection and performs insert statements.
```bash
java -cp .:target/htapbench-0.95-jar-with-dependencies.jar pt.haslab.htapbench.core.HTAPBench -b database_name -c your_config_file.xml --load true --execute false --calibrate true
```

# C. Run Tests
Before running any tests ensure that the previous stage was successfully completed. 

Then run:
```bash
java -cp .:target/htapbench-0.95-jar-with-dependencies.jar pt.haslab.htapbench.core.HTAPBench -b database_name -c config/htapb_config_postgres.xml --create false --load false --execute true --s 120 --calibrate false
```

# Publications
If you are using this benchmark for your papers or for your work, please cite the paper:

HTAPBench: Hybrid Transactional and Analytical Processing Benchmark 
Fábio Coelho, João Paulo, Ricardo Vilaça, José Pereira, Rui Oliveira
Proceedings of the 8th ACM/SPEC on International Conference on Performance Engineering

BibTex:
```bash
@inproceedings{Coelho:2017:HHT:3030207.3030228,
 author = {Coelho, F\'{a}bio and Paulo, Jo\~{a}o and Vila\c{c}a, Ricardo and Pereira, Jos{\'e} and Oliveira, Rui},
 title = {HTAPBench: Hybrid Transactional and Analytical Processing Benchmark},
 booktitle = {Proceedings of the 8th ACM/SPEC on International Conference on Performance Engineering},
 series = {ICPE '17},
 year = {2017},
 isbn = {978-1-4503-4404-3},
 location = {L'Aquila, Italy},
 pages = {293--304},
 numpages = {12},
 url = {http://doi.acm.org/10.1145/3030207.3030228},
 doi = {10.1145/3030207.3030228},
 acmid = {3030228},
 publisher = {ACM},
 address = {New York, NY, USA},
 keywords = {benchmarking, htap, olap, oltp},
} 
```



