from pyspark.sql import SparkSession
from pysparklink.core import SparkLink
from pysparklink.core import LinkConfig
from splink.spark import SparkLinker

spark = SparkSession.builder.appName("SparkLink Batch").getOrCreate()

# Configure SparkLink
link_config = LinkConfig(
    spark=spark,
    kafka_bootstrap_servers="localhost:29092",
    kafka_topic="topic1",
    kafka_group_id="sparklink-group",
    kafka_offset_earliest=True,
    kafka_max_poll_records=1000
)

# Create a SparkLink instance
link = SparkLink(link_config)

# Process the data in batch mode
df = link.process_batch(
    output_mode="append",
    output_path="output"
)

# Create a SparkLinker instance
linker = SparkLinker()

# Link the data using Apache Splink
silver_df = linker.link_df(
    df,
    settings_dict={
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l firstname, surname, birthdate",
            "l surname, firstname, birthdate",
            "l birthdate, surname, firstname"
        ],
        "retain_intermediate_calcuations": True
    }
)

# Save the silver layer to parquet
silver_df.write.parquet("silver_layer", mode="overwrite")

# Link the data using Apache Splink again to create a gold layer
gold_df = linker.link_df(
    silver_df,
    settings_dict={
        "link_type": "cluster",
        "linking_function": "clustering.cluster_at_threshold",
        "comparison_columns": [
            "cluster_id"
        ],
        "retain_intermediate_calcuations": True
    }
)

# Save the gold layer to parquet
gold_df.write.parquet("gold_layer", mode="overwrite")
