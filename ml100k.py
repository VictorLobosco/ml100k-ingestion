from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.window import *
import argparse



spark = SparkSession.builder.appName("ml-100k").getOrCreate()

ocupaschema = StructType([StructField("Occupation", StringType(),True)])

genreschema = StructType([\
                            StructField("Genre", StringType(),True), \
])

userschema = StructType([\
                            StructField("id", IntegerType(), True),   \
                            StructField("Age", IntegerType(), True),   \
                            StructField("Gender", StringType(), True),  \
                            StructField("Occupation", StringType(), True),\
                            StructField("Zipcode", StringType(), True)])

itenschema = StructType([\
                            StructField("id", IntegerType(), True), \
                            StructField("Name", StringType(), True), \
                            StructField("Date", StringType(), True),  \
                            StructField("Null", StringType(), True),  \
                            StructField("Link", StringType(), True),\
                            StructField("unknown", IntegerType(), True), \
                            StructField("Action", IntegerType(), True), \
                            StructField("Adventure", IntegerType(), True), \
                            StructField("Animation", IntegerType(), True), \
                            StructField("Childrens", IntegerType(), True), \
                            StructField("Comedy", IntegerType(), True), \
                            StructField("Crime", IntegerType(), True), \
                            StructField("Documentary", IntegerType(), True), \
                            StructField("Drama", IntegerType(), True), \
                            StructField("Fantasy", IntegerType(), True), \
                            StructField("FilmNoir", IntegerType(), True), \
                            StructField("Horror", IntegerType(), True), \
                            StructField("Musical", IntegerType(), True), \
                            StructField("Mystery", IntegerType(), True), \
                            StructField("Romance", IntegerType(), True), \
                            StructField("SciFi", IntegerType(), True), \
                            StructField("Thriller", IntegerType(), True), \
                            StructField("War", IntegerType(), True), \
                            StructField("Western", IntegerType(), True)])

moviexgenreschema = StructType([\
                            StructField("Movie_id", IntegerType(), True), \
                            StructField("Genre_id", IntegerType(), True)
])

scoreschema = StructType([\
                            StructField("User_id", IntegerType(), True), \
                            StructField("Movie_id", IntegerType(), True), \
                            StructField("Rating", IntegerType(), True), \
                            StructField("Timestamp", LongType(), True)
])



occupation = spark.read.schema(ocupaschema).csv("file:///ml-100k/u.occupation")

Genre = spark.read.option("sep","|").schema(genreschema).csv("file:///ml-100k/u.genre")

user = spark.read.option("sep","|").schema(userschema).csv("file:///ml-100k/u.user")

score = spark.read.option("sep", "\t").schema(scoreschema).csv("file:///ml-100k/u.data")

itens = spark.read\
    .option("sep","|")\
    .option("charset", "ISO-8859-1")\
    .schema(itenschema).csv("file:///ml-100k/u.item")

#code that allow arguments to be passed when calling the script, they are used to access the database in which the data is going to be written in
parser = argparse.ArgumentParser()
parser.add_argument('--url')
parser.add_argument('--schema')
parser.add_argument('--user')
parser.add_argument('--password')
args = parser.parse_args()

#uses the window module to create the colum id of the occupation table
window = Window.orderBy(func.col("Occupation"))
occupation = occupation.withColumn('id', func.row_number().over(window))

#uses the window module to create the colum id of the movie genre table
window = Window.orderBy(func.col("Genre"))
Genre = Genre.withColumn('id', func.row_number().over(window))

#create an alias to both dataframes, this will be usefull when whe join both and want to rename the id table that got joined
user = user.alias('user')
occupation = occupation.alias('occupation')
#the actual join code
user = user.join(occupation, on= 'occupation', how='left')
#drop the occupation table as this is not useful here
user = user.drop('Occupation')
#this select is used to rename the id that got joined from the occupation table, here is where the alias given to the dataframes are used
user = user.select(
    func.col("user.id"),
    func.col("user.Age"),
    func.col("user.Gender"),
    func.col("user.Zipcode"),
    func.col("occupation.id").alias("Occupation_id")
)
#uses a regular expression to replace the releases date that was in the movie name colum with an empty space
itens = itens.withColumn('Name', func.regexp_replace(func.col("Name"),  r"\((\d+[^()]+)\)", ''))



#code that creates a dictionary for each movie id and its respective genres, the !=0 is used because the entrys that were not in the genre would be added to dictionary because of how the genre are stored in the original file
ditcAc = {row['id']:row['Action'] for row in itens.collect() if row['Action'] != 0}
ditcAd = {row['id']:row['Adventure'] for row in itens.collect() if row['Adventure'] != 0}
ditcAn = {row['id']:row['Animation'] for row in itens.collect() if row['Animation'] != 0}
ditcCh = {row['id']:row['Childrens'] for row in itens.collect() if row['Childrens'] != 0}
ditcCo = {row['id']:row['Comedy'] for row in itens.collect() if row['Comedy'] != 0}
ditcCr = {row['id']:row['Crime'] for row in itens.collect() if row['Crime'] != 0}
ditcDo = {row['id']:row['Documentary'] for row in itens.collect() if row['Documentary'] != 0}
ditcDr = {row['id']:row['Drama'] for row in itens.collect() if row['Drama'] != 0}
ditcFa = {row['id']:row['Fantasy'] for row in itens.collect() if row['Fantasy'] != 0}
ditcFi = {row['id']:row['FilmNoir'] for row in itens.collect() if row['FilmNoir'] != 0}
ditcHo = {row['id']:row['Horror'] for row in itens.collect() if row['Horror'] != 0}
ditcMu = {row['id']:row['Musical'] for row in itens.collect() if row['Musical'] != 0}
ditcMy = {row['id']:row['Mystery'] for row in itens.collect() if row['Mystery'] != 0}
ditcRo = {row['id']:row['Romance'] for row in itens.collect() if row['Romance'] != 0}
ditcSc = {row['id']:row['SciFi'] for row in itens.collect() if row['SciFi'] != 0}
ditcTh = {row['id']:row['Thriller'] for row in itens.collect() if row['Thriller'] != 0}
ditcWa = {row['id']:row['War'] for row in itens.collect() if row['War'] != 0}
ditcWe = {row['id']:row['Western'] for row in itens.collect() if row['Western'] != 0}
ditcUn = {row['id']:row['unknown'] for row in itens.collect() if row['unknown'] != 0}

#replace the value of dictionary (which at this moment is 1) to the same as its id counterpart in the genre table!
ditcAc = {x: 1 for x in ditcAc}
ditcAd = {x: 2 for x in ditcAd}
ditcAn = {x: 3 for x in ditcAn}
ditcCh = {x: 4 for x in ditcCh}
ditcCo = {x: 5 for x in ditcCo}
ditcCr = {x: 6 for x in ditcCr}
ditcDo = {x: 7 for x in ditcDo}
ditcDr = {x: 8 for x in ditcDr}
ditcFa = {x: 9 for x in ditcFa}
ditcFi = {x: 10 for x in ditcFi}
ditcHo = {x: 11 for x in ditcHo}
ditcMu = {x: 12 for x in ditcMu}
ditcMy = {x: 13 for x in ditcMy}
ditcRo = {x: 14 for x in ditcRo}
ditcSc = {x: 15 for x in ditcSc}
ditcTh = {x: 16 for x in ditcTh}
ditcWa = {x: 17 for x in ditcWa}
ditcWe = {x: 18 for x in ditcWe}
ditcUn = {x: 19 for x in ditcUn}

#creats a list using the dictionary created, thoses lists will be merged toghter to be used in the creation of the dataframe containing the genres that the movies are in
liUn = list(ditcUn.items())
liAc = list(ditcAc.items())
liAd = list(ditcAd.items())
liAn = list(ditcAn.items())
liCh = list(ditcCh.items())
liCr = list(ditcCr.items())
liDo = list(ditcDo.items())
liDr = list(ditcDr.items())
liFa = list(ditcFa.items())
liFi = list(ditcFi.items())
liHo = list(ditcHo.items())
liMu = list(ditcMu.items())
liMy = list(ditcMy.items())
liRo = list(ditcRo.items())
liSc = list(ditcSc.items())
liTh = list(ditcTh.items())
liWa = list(ditcWa.items())
liWe = list(ditcWe.items())

#the code that merges the lists
listtodf = liUn + liAc + liAd + liAn + liCh + liCr + liDo + liDr + liFa + liFi + liHo + liMu + liMy + liRo + liSc + liTh + liWa + liWe
#creates the dataframe using the finalized list and uses the 'moviexgenreschema' schema
moviexgenre = spark.createDataFrame(data =listtodf, schema = moviexgenreschema)
# a simple window function to give the entrys its respective id
window = Window.orderBy(func.col("Movie_id"))
moviexgenre = moviexgenre.withColumn('id', func.row_number().over(window))

#puts the id as the first item in the dataframe
moviexgenre = moviexgenre.select(
    func.col("id"),
    func.col("Movie_id"),
    func.col("Genre_id")
)
#creates the movie dataframe from the itens dataframe. that dataframe contained a lot of data that is not used in the soon to be movie table so whe use a select to just add the data whe need
movie = itens.select(
    func.col("id"),
    func.col("Name"),
    func.col("Date"),
    func.col("Link").alias("IMDB_Url")
)
# a simple window function to give the entrys its respective id
window = Window.orderBy(func.col("timestamp"))
score = score.withColumn('id', func.row_number().over(window))

#this select is used to convert the timestamp colum who originaly is on unix time to timestamp type
score = score.select(
    func.col("id"),
    func.col("User_id"),
    func.col("Movie_id"),
    func.col("Rating"),
    func.from_unixtime(func.col("timestamp")).alias("Timestamp")
)
#JDBC code to write on the database
# the URL, SCHEMA, USER and PASSWORD are setted by passing them as arguments when calling the script.
#Example: spark-submit --driver-class-path C:\ml100k\postgresql-42.2.23.jar project.py --url jdbc:postgresql://localhost:5432/postgres --schema ml100k --user postgres --password admin
occupation.write \
    .format("jdbc") \
    .option("url", args.url) \
    .option("dbtable", args.schema + '.Occupation') \
    .option("user", args.user) \
    .option("password", args.password) \
    .mode ("overwrite") \
    .save()

Genre.write \
    .format("jdbc") \
    .option("url", args.url) \
    .option("dbtable", args.schema + '.Genre') \
    .option("user", args.user) \
    .option("password", args.password) \
    .mode ("overwrite") \
    .save()

user.write \
    .format("jdbc") \
    .option("url", args.url) \
    .option("dbtable", args.schema + '.User') \
    .option("user", args.user) \
    .option("password", args.password) \
    .mode ("overwrite") \
    .save()

movie.write \
    .format("jdbc") \
    .option("url", args.url) \
    .option("dbtable", args.schema + '.Movie') \
    .option("user", args.user) \
    .option("password", args.password) \
    .mode ("overwrite") \
    .save()

moviexgenre.write \
    .format("jdbc") \
    .option("url", args.url) \
    .option("dbtable", args.schema + '.MoviexGenre') \
    .option("user", args.user) \
    .option("password", args.password) \
    .mode ("overwrite") \
    .save()

score.write \
    .format("jdbc") \
    .option("url", args.url) \
    .option("dbtable", args.schema + '.Score') \
    .option("user", args.user) \
    .option("password", args.password) \
    .mode ("overwrite") \
    .save()
