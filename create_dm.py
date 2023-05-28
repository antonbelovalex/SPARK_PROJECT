import configparser
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import StringType, StructField, StructType

def create_datamart():
    # Конфигурация логирования
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger(__name__)

    try:

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

        logger.info("Загружены данные из Витрин")

        # Чтение данных из Витрины A
        data_a = spark.read.parquet("путь_к_витрине_A")

        # Определение схемы для результирующей витрины
        data_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("inn", StringType(), nullable=True),
            StructField("hash", StringType(), nullable=True),
            StructField("phone", StringType(), nullable=True),
            StructField("site_id", StringType(), nullable=True),
            StructField("cookie", StringType(), nullable=True),
            StructField("gaid_idfa", StringType(), nullable=True)
        ])

        # Выбор необходимых столбцов из Витрины A
        data_selected = data_a.select(
            col("raw_cookie"),
            col("data_value"),
            col("geocountry"),
            col("city"),
            col("user_uid"),
            col("match_code"),
            col("timestampcolumn")
        )

        # Развертывание массива структур raw_cookie
        data_exploded = data_selected.withColumn("cookie", explode(col("raw_cookie")))

        # Выбор нужных столбцов и переименование столбцов
        data_transformed = data_exploded.select(
            col("user_uid").alias("user_id"),
            col("cookie.value").alias("cookie"),
            col("data_value").alias("hash"),
            lit(None).alias("inn"),
            lit(None).alias("phone"),
            lit(None).alias("site_id"),
            lit(None).alias("gaid_idfa")
        ).distinct()

        # Объединение с данными из других витрин (B, C, D, E, F, G)
        data_b = spark.read.parquet("путь_к_витрине_B")
        data_c = spark.read.parquet("путь_к_витрине_C")
        data_d = spark.read.parquet("путь_к_витрине_D")
        data_e = spark.read.parquet("путь_к_витрине_E")
        data_f = spark.read.parquet("путь_к_витрине_F")
        data_g = spark.read.parquet("путь_к_витрине_G")

        data_union = data_transformed.union(data_b).union(data_c).union(data_d).union(data_e).union(data_f).union(data_g)
        
        logger.info("Удаляем дубликаты")
        data_union = data_union.dropDuplicates()
        logger.info("Дубликаты удалены")
        data_union = data_union.cache()  # Кэширование таблицы
        data_union = data_union.repartition(10)  # Перепартиционирование таблицы
        # Запись результирующей витрины в партиционированный формат
        data_union.write.partitionBy("timestampcolumn").parquet("путь_к_витрине_результат")
        logger.info("Записана результирующая витрина")
        spark.stop()

    except Exception as e:
            logger.error("Произошла ошибка: {}".format(str(e)))
            raise

create_datamart()
