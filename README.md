# ml100k-ingestion

A pyspark project that uses the ml100k dataset to create a dimensional database in postgress.

This project was created to help me get used to using pyspark.

# Running the code

To run the code you need to either have the ml100k folder on the root of the C: driver or modify the code to use the location of the dataset in your computer.

When you run the code you need to specify the location of the postgress driver the URL, SCHEMA, USER and PASSWORD, that is done using the commands:

- --driver-class-path For the postgress driver.

- --url For database.

- --schema For the schema that will be used, you will have to use a already existing schema as the code does not create one.

- --user For the user to login in the database

- --password For the password to login in the database.



Here is a example of how to run the code, just change the paths to the ones you will use:

spark-submit --driver-class-path C:\Spark\postgresql-42.2.23.jar ml100k.py --url jdbc:postgresql://localhost:5432/ml100k --schema ml100k --user postgres --password admin


The ml100k dataset can be obtained in this link: https://grouplens.org/datasets/movielens/100k/
