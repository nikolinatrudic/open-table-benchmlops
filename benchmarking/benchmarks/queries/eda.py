from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, countDistinct, month, sum, year


def get_basic_overview_query(df: DataFrame):
    return df.select(
        countDistinct("InvoiceNo").alias("total_invoices"),
        avg("UnitPrice").alias("avg_unit_price"),
        sum(col("Quantity") * col("UnitPrice")).alias("total_sales"),
    )


def get_top_selling_products(df: DataFrame):
    return (
        df.groupBy("Description")
        .agg(sum("Quantity").alias("total_quantity_sold"))
        .orderBy("total_quantity_sold", ascending=False)
        .limit(5)
    )


def get_sales_over_time(df: DataFrame):
    return (
        df.withColumn("year", year("InvoiceDate"))
        .withColumn("month", month("InvoiceDate"))
        .groupBy("year", "month")
        .agg(sum(col("Quantity") * col("UnitPrice")).alias("monthly_sales"))
        .orderBy("year", "month")
    )


def get_customer_analysis(df: DataFrame):
    return (
        df.where(df["CustomerID"].isNotNull())
        .groupBy("CustomerID")
        .agg(sum(col("Quantity") * col("UnitPrice")).alias("total_spend"))
        .orderBy("total_spend", ascending=False)
        .limit(5)
    )


def get_country_wise_analysis(df: DataFrame):
    return (
        df.groupBy("Country")
        .agg(sum(col("Quantity") * col("UnitPrice")).alias("country_sales"))
        .orderBy("country_sales", ascending=False)
    )


def get_eda_queries(df: DataFrame) -> dict:
    return {
        "Basic Overview": get_basic_overview_query(df),
        "Top Selling Products": get_top_selling_products(df),
        "Sales Over Time": get_sales_over_time(df),
        "Customer Analysis": get_customer_analysis(df),
        "Country Wise Analysis": get_country_wise_analysis(df),
    }
