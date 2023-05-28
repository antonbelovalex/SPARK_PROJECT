import configparser
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StructType
from faker import Faker

 # Загрузка конфигурации Spark из файла
config = configparser.ConfigParser()
config.read("spark_config.ini")
spark_config = config["SparkConfig"]

spark = SparkSession.builder \
    .appName("DataMartCreation") \
    .config("spark.sql.shuffle.partitions", spark_config["spark.sql.shuffle.partitions"]) \
    .config("spark.executor.memory", spark_config["spark.executor.memory"]) \
    .config("spark.driver.memory", spark_config["spark.driver.memory"]) \
    .getOrCreate()
faker = Faker()

# Генерация данных для Витрины A
data_a = spark.createDataFrame([
    ([
        {"key": "_sa_cookie_a", "value": faker.uuid4()},
        {"key": "_fa_coookie_b", "value": faker.uuid4()},
        {"key": "_ym_cookie_c", "value": faker.random_number(digits=20)},
        {"key": "_fbp", "value": faker.uuid4()},
        {"key": "org_uid", "value": faker.random_number(digits=7)},
        {"key": "user_uid", "value": faker.random_number(digits=7)},
        {"key": "user_phone", "value": faker.phone_number()},
        {"key": "user_mail", "value": faker.email()}
    ], "SUBMIT", "pageview", faker.sha256(), faker.country(), faker.city(), faker.user_agent(),
     faker.random_element(["RU", "ENG"]), faker.random_number(digits=6), faker.random_element(["WEB", "MOBILE"]),
     faker.random_int(min=800, max=3840), faker.date_time_between(start_date='-1y', end_date='now'))
], StructType([
    StructField("raw_cookie", ArrayType(StructType([
        StructField("key", StringType(), nullable=False),
        StructField("value", StringType(), nullable=False)
    ])), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_action", StringType(), nullable=False),
    StructField("data_value", StringType(), nullable=False),
    StructField("geocountry", StringType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("user_os", StringType(), nullable=False),
    StructField("systemlanguage", StringType(), nullable=False),
    StructField("geoaltitude", StringType(), nullable=False),
    StructField("meta_platform", StringType(), nullable=False),
    StructField("screensize", StringType(), nullable=False),
    StructField("timestampcolumn", TimestampType(), nullable=False)
]))

# Генерация данных для Витрины B
data_b = spark.createDataFrame([(faker.uuid4(), faker.random_number(digits=10))],
                               StructType([StructField("id", StringType(), nullable=False),
                                           StructField("inn", StringType(), nullable=False)]))

# Генерация данных для Витрины C
data_c = spark.createDataFrame([(faker.uuid4(), faker.uuid4())],
                               StructType([StructField("id", StringType(), nullable=False),
                                           StructField("cookie_a", StringType(), nullable=False)]))

# Генерация данных для Витрины D
data_d = spark.createDataFrame([(faker.uuid4(), faker.random_number(digits=10))],
                               StructType([StructField("id", StringType(), nullable=False),
                                           StructField("inn", StringType(), nullable=False)]))

# Генерация данных для Витрины E
data_e = spark.createDataFrame([(faker.uuid4(), faker.md5())],
                               StructType([StructField("id", StringType(), nullable=False),
                                           StructField("hash_phone_md5", StringType(), nullable=False)]))

# Генерация данных для Витрины F
data_f = spark.createDataFrame([(faker.uuid4(), faker.md5())],
                               StructType([StructField("id", StringType(), nullable=False),
                                           StructField("hash_email_md5", StringType(), nullable=False)]))

# Генерация данных для Витрины G
data_g = spark.createDataFrame([(faker.random_element(["GAID", "IDFA"]), faker.uuid4(), faker.uuid4())],
                               StructType([StructField("match_code", StringType(), nullable=False),
                                           StructField("user_uid", StringType(), nullable=False),
                                           StructField("id", StringType(), nullable=False)]))

data_a.show(truncate=False)
data_b.show(truncate=False)
data_c.show(truncate=False)
data_d.show(truncate=False)
data_e.show(truncate=False)
data_f.show(truncate=False)
data_g.show(truncate=False)
