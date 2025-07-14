# Databricks notebook source

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType


# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "9e916e80-f3cc-4a05-9e98-b966a1226a92",
  "fs.azure.account.oauth2.client.secret": "rUL8Q~FggFW8JUFX0KB~gz13qpT70N_8S-w5obG.",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/2f637f16-90ec-4141-8d43-7fe50e9d8f03/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://tokyo-olympic-data@tokyoolmpicdeepak1.dfs.core.windows.net/",
  mount_point = "/mnt/tokyoolympic",
  extra_configs = configs
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/tokyoolympic"

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/athletes.csv")
athletes.show()

# COMMAND ----------

athletes = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/athletes.csv")
Coaches = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/Coaches.csv")
EntriesGender = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/EntriesGender.csv")
Medals = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/Medals.csv")
Teams = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/Teams.csv")



# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

Coaches.printSchema()


# COMMAND ----------

Coaches = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/Coaches.csv")
Coaches.show()


# COMMAND ----------

EntriesGender = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/EntriesGender.csv")
EntriesGender.show()


# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

EntriesGender = EntriesGender \ 
    withColumn("Female", col("Female").cast("Integer")) \
    withColumn("Male",col("Male").cast("Integer")) \
    withColumn("Total",col("Total").cast("Integer"))
EntriesGender.show()

# COMMAND ----------

EntriesGender = EntriesGender \
    .withColumn("Female", col("Female").cast("Integer")) \
    .withColumn("Male", col("Male").cast("Integer")) \
    .withColumn("Total", col("Total").cast("Integer"))

EntriesGender.show()

# COMMAND ----------

EntriesGender.printSchema()


# COMMAND ----------

Medals.printSchema()

# COMMAND ----------

Medals = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/Medals.csv")
Medals.show()

# COMMAND ----------

Medals = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("dbfs:/mnt/tokyoolympic/raw data/Medals.csv")
    Medals.show()

# COMMAND ----------

medals.printSchema()


# COMMAND ----------

Medals.printSchema() 

# COMMAND ----------

tems 

# COMMAND ----------

Teams.show()

# COMMAND ----------

Teams.printSchema()

# COMMAND ----------

Teams.show()

# COMMAND ----------

top_gold_medal = Medals.orderBy("gold", ascending=False)
top_gold_medal.show()

# COMMAND ----------

top_gold_medal = Medals.orderBy("silver", ascending=False).show()

# COMMAND ----------

top_gold_medal = Medals.orderBy("gold", ascending=False).show()

# COMMAND ----------

top_gold_medal = Medals.orderBy("gold", ascending=False).select("Team_country","gold").show()

# COMMAND ----------

athletes.write.option("header",'true').csv("dbfs:/mnt/tokyoolympic/transformed data/athletes")

# COMMAND ----------

athletes.write.mode("overwrite").option("header",'true').csv("dbfs:/mnt/tokyoolympic/transformed data/athletes")

# COMMAND ----------

Coaches.write.mode("overwrite").option("header", "true").csv("dbfs:/mnt/tokyoolympic/transformed data/Coaches")
EntriesGender.write.mode("overwrite").option("header", "true").csv("dbfs:/mnt/tokyoolympic/transformed data/EntriesGender")
Medals.write.mode("overwrite").option("header", "true").csv("dbfs:/mnt/tokyoolympic/transformed data/Medals")
Teams.write.mode("overwrite").option("header", "true").csv("dbfs:/mnt/tokyoolympic/transformed data/Teams")

# COMMAND ----------

Coaches.show()

# COMMAND ----------

athletes = spark.read.format("csv") \
    .option("header", "true") \
    .load("dbfs:/mnt/tokyoolympic/raw data/athletes.csv")


# COMMAND ----------

athletes.show()

# COMMAND ----------

Coaches.show()

# COMMAND ----------

Coaches = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/Coaches.csv")
EntriesGender = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/EntriesGender.csv")
Medals = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/Medals.csv")
Teams = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/tokyoolympic/raw data/Teams.csv")

# COMMAND ----------

Teams.show()

# COMMAND ----------

