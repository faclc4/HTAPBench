# HTAPBench

The Hybrid Transactional and Analytical Processing Benchmark is targeted at assessing engines capable of delivering mixed workloads composed of OLTP transactions and OLAP business queries without resorting to ETL.

There are a few requirements to run HTAPBench:
1. You need to have JAVA distribuiton installed on the machine running HTAPBench.
2. You need to have installed the JDBC driver for the databse you want to test.

===

# A. To compile HTAPBench:
''' bash
	mvn clean compile package
'''

# B. Configure HTAPBench:

1. Clone and adjust the configuration file in config/htapb_config_postgres.xml to you test case.

Before running HTAPBench, you will need to load data into the database.

0. Before you continue ensure that:
	- The database engine you wish to test is installed and that you reach it from the machine running HTAPBench.
	- In the database engine to be tested, create a test database e.g., htapb.
	- In the database engine to be tested, create a user/password and grant all priviledges to your test database.
	- In the database engine to be tested, install the database schema.
	 ''' bash
	 	java -cp .:target/htapbench-0.95-jar-with-dependencies.jar pt.haslab.htapbench.core.HTAPBench -b htapb -c config/htapb_config_postgres.xml --create true --load false --generateFiles false --filePath ~/Desktop/ --execute false --calibrate false
	 '''

# C. Populate

You have 2 choices:
	(a) Generate the CSV files to populate the database. (We recommend this method as it usually loads data faster.)
		''' bash
			java -cp .:target/htapbench-0.95-jar-with-dependencies.jar pt.haslab.htapbench.core.HTAPBench -b htapb -c your_config_file.xml --generateFiles true --filePath dir --execute false --calibrate true

		'''
		(Provide the location for your configuration file and for the directory where the files will be placed)

	(b) 





