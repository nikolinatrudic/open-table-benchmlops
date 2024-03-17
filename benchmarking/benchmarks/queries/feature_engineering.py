from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    max,
    sum,
)


def get_avg_spend_per_invoice(df: DataFrame):
    avg_spend = df.groupBy("InvoiceNo").agg(
        avg(col("Quantity") * col("UnitPrice")).alias("avg_invoice_spend")
    )
    return df.join(avg_spend, on="InvoiceNo", how="left")


def get_customer_lifetime_value(df: DataFrame):
    clv = df.groupBy("CustomerID").agg(
        sum(col("Quantity") * col("UnitPrice")).alias("customer_lifetime_value")
    )
    return df.join(clv, on="CustomerID", how="left")


def get_product_popularity_score(df: DataFrame):
    popularity = df.groupBy("Description").agg(
        sum("Quantity").alias("total_quantity"),
        countDistinct("InvoiceNo").alias("invoice_count"),
        (sum("Quantity") * countDistinct("InvoiceNo")).alias("popularity_score"),
    )
    return df.join(popularity, on="Description", how="left")


def get_recency_of_purchase(df: DataFrame):
    recency = df.groupBy("CustomerID").agg(
        max("InvoiceDate").alias("last_purchase_date")
    )
    return df.join(recency, on="CustomerID", how="left")


def get_frequency_of_purchases(df: DataFrame):
    frequency = df.groupBy("CustomerID").agg(
        countDistinct("InvoiceNo").alias("purchase_frequency")
    )
    return df.join(frequency, on="CustomerID", how="left")


def get_rolling_window_feature(df: DataFrame):
    windowSpec = (
        Window.partitionBy("CustomerID").orderBy("InvoiceDate").rangeBetween(-6, 0)
    )
    rolling_sum = df.withColumn(
        "rolling_quantity_sum", sum("Quantity").over(windowSpec)
    )
    return rolling_sum


def get_feature_engineering_queries(df: DataFrame) -> dict:
    return {
        "Avg Spend per Invoice + Original": get_avg_spend_per_invoice(df),
        "Customer Lifetime Value + Original": get_customer_lifetime_value(df),
        "Product Popularity Score + Original": get_product_popularity_score(df),
        "Recency of Purchase + Original": get_recency_of_purchase(df),
        "Frequency of Purchases + Original": get_frequency_of_purchases(df),
        # "Rolling Window Feature": get_rolling_window_feature(df)
    }
