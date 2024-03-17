from pydantic_settings import BaseSettings


class BenchmarkSettings(BaseSettings):
    data_path: str = "s3a://data"
    csv_dataset_path: str = f"{data_path}/raw/OnlineRetail.csv"
    delta_table: str = "DeltaOnlineRetail"
    iceberg_table: str = "IcebergOnlineRetail"
    hudi_table: str = "HudiOnlineRetail"
    partition_column: str = "Date"


benchmark_settings = BenchmarkSettings()
