from pyspark.sql import DataFrame
from pyspark.sql.functions import col, datediff, lit, when
from pyspark.sql.window import Window


def create_target_variable(df: DataFrame):
    # Assuming 'CustomerID' and 'InvoiceDate' are present
    current_date = df.select(max("InvoiceDate")).collect()[0][
        0
    ]  # Assuming current_date is the maximum date in your dataset

    # Calculate the days since the last purchase for each customer
    windowSpec = Window.partitionBy("CustomerID")

    # Create a new column with the last purchase date for each customer
    df = df.withColumn(
        "last_purchase_date",
        max(col("InvoiceDate")).over(windowSpec),  # type: ignore
    )
    df = df.withColumn(
        "days_since_last_purchase", datediff(lit(current_date), "last_purchase_date")
    )

    # Define churned customers based on the threshold (e.g., no purchase in the last 180 days)
    churn_threshold = 180
    df = df.withColumn(
        "target",
        when(col("days_since_last_purchase") > churn_threshold, 1).otherwise(0),
    )
    return df


def split_features_and_target(df: DataFrame, target_column_name: str):
    X = df.drop(target_column_name)
    y = df.select(target_column_name)
    return X, y


def split_dataset(df: DataFrame, train_ratio=0.8):
    """
    Splits the dataset into training and testing sets based on the specified ratio.
    """
    train_df, test_df = df.randomSplit([train_ratio, 1 - train_ratio], seed=42)
    return train_df, test_df


def prepare_dataset_for_training(df: DataFrame):
    # Assuming df already includes all necessary feature engineering from previous steps
    # TODO - all steps here save from previous part

    df = create_target_variable(df)
    X, y = split_features_and_target(df, "target")

    return X, y


def get_model_training_queries(df):
    """
    Returns a dictionary of functions related to preparing the dataset for model training.
    """
    return {
        "Create Target Variable": create_target_variable(df),
        "Split Features and Target": split_features_and_target(df),
        "Split Dataset": split_dataset(df),
        "Prepare Dataset for Training": prepare_dataset_for_training(df),
    }
