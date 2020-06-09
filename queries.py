#!/usr/bin/env python

def basic_query(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a dataframe corresponding to the
    first five people, ordered alphabetically by last_name, first_name.

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the CSV file, e.g.,
        `hdfs:/user/bm106/pub/people_small.csv`

    schema : string
        The CSV schema
    '''

    # This loads the CSV file with proper header decoding and schema
    people = spark.read.csv(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')

    top5 = spark.sql('SELECT * FROM people ORDER BY last_name, first_name ASC LIMIT 5')

    return top5

# --- ADD YOUR NEW QUERIES BELOW ---
#


def csv_avg_income(spark, file_path):
    
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')

    avg_income = spark.sql('SELECT zipcode, AVG(income) FROM people GROUP BY zipcode')

    return avg_income

    # TODO:
  


def csv_max_income(spark, file_path):
    
    # TODO:
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')

    max_income = spark.sql('SELECT last_name, MAX(income) FROM people GROUP BY last_name')

    return max_income
   


def csv_anna(spark, file_path):
    
    # TODO:
    people = spark.read.csv(file_path, header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')

    people.createOrReplaceTempView('people')

    anna = spark.sql('SELECT * FROM people WHERE (first_name = "Anna" AND income >= 70000)')

    return anna



def pq_avg_income(spark, file_path):
    
    # TODO:
    people = spark.read.parquet(file_path)
    people.createOrReplaceTempView('people')
    
    avg_income = spark.sql('SELECT zipcode, AVG(income) FROM people GROUP BY zipcode')

    return avg_income


def pq_max_income(spark, file_path):
    
    # TODO:
    people = spark.read.parquet(file_path)
    people.createOrReplaceTempView('people')

    max_income = spark.sql('SELECT last_name, MAX(income) FROM people GROUP BY last_name')

    return max_income



def pq_anna(spark, file_path):
    
    # TODO:
    people = spark.read.parquet(file_path)
    people.createOrReplaceTempView('people')

    anna = spark.sql('SELECT * FROM people WHERE (first_name = "Anna" AND income >= 70000)')

    return anna

