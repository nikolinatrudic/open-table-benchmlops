import random

from faker import Faker
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

fake = Faker()

product_stock_map = {
    "10001": "Red T-Shirt",
    "10002": "Blue Jeans",
    "10003": "Leather Wallet",
    "10004": "Running Shoes",
    "10005": "Baseball Cap",
    "10006": "Sunglasses",
    "10007": "Wristwatch",
    "10008": "Backpack",
    "10009": "Winter Coat",
    "10010": "Swim Shorts",
}

schema = StructType(
    [
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("UnitPrice", FloatType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("Country", StringType(), True),
    ]
)


def create_fake_transaction() -> tuple:
    """Create one fake transaction.

    Returns:
        tuple: Tuple which contains generated transaction data.
    """
    stock_code, product_description = random.choice(list(product_stock_map.items()))

    invoice_number = "".join([str(random.randint(0, 9)) for _ in range(6)])
    if random.random() < 0.05:
        invoice_number = invoice_number + "C"

    return (
        invoice_number,
        stock_code,
        product_description,
        random.randint(1, 20),
        fake.date_time_between(start_date="-15y", end_date="now"),
        round(random.uniform(0.1, 100.0), 2),
        fake.random_int(min=10000, max=99999),
        fake.country(),
    )


def generate_fake_transactions(num_records: int, spark: SparkSession) -> DataFrame:
    """Generate DataFrame of fake transactions.

    Args:
        num_records (int): Number of transactions to generate
        spark (SparkSession): Spark Session instance

    Returns:
        DataFrame: Spark DataFrame with fake transactions
    """
    generate_transaction = udf(create_fake_transaction, schema)
    dummy_data = spark.range(num_records)
    data = (
        dummy_data.withColumn("temp", lit(1))
        .select(generate_transaction().alias("data"))
        .select("data.*")
    )
    logger.info("Fake data generation complete.")
    return data


if __name__ == "__main__":
    spark = SparkSession.builder.appName("FakeTransactionGeneration").getOrCreate()
    for i in range(3):
        num_records = random.randint(10000, 1000000)
        fake_transaction_data = generate_fake_transactions(
            num_records=num_records, spark=spark
        )
        fake_transaction_data.toPandas().to_csv(  # type: ignore
            f"./data/raw/FakeTransactionData{i}.csv", index=False
        )
