from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType  # Import schema types
from pyspark.sql.functions import col, explode
import json

def load_avro_schema(schema_path):
    """
    Load AVRO schema from an AVSC file.
    """
    with open(schema_path, "r") as file:
        avro_schema = json.load(file)
    return avro_schema

def flatten_struct(df):
    """
    Flatten nested structures in a DataFrame.
    """
    def flatten_columns(schema, prefix=""):
        fields = []
        for field in schema.fields:
            name = f"{prefix}.{field.name}" if prefix else field.name
            if isinstance(field.dataType, StructType):
                fields += flatten_columns(field.dataType, name)
            elif isinstance(field.dataType, ArrayType):
                fields.append((name, explode(col(name)).alias(name.replace(".", "_"))))
            else:
                fields.append((name, col(name).alias(name.replace(".", "_"))))
        return fields

    flat_columns = flatten_columns(df.schema)
    for name, expression in flat_columns:
        df = df.withColumn(name.replace(".", "_"), expression)
    return df.select(*[col(name.replace(".", "_")) for name, _ in flat_columns])

def main(input_path, schema_path, output_path, output_format):
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Avro to Flattened JSON/CSV with Schema") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.2") \
        .getOrCreate()

    # Load AVRO schema
    avro_schema = load_avro_schema(schema_path)

    # Read AVRO file with schema
    df = spark.read.format("avro") \
        .option("avroSchema", json.dumps(avro_schema)) \
        .load(input_path)

    df.show()

    # Flatten the DataFrame
    flat_df = flatten_struct(df)

    # Write output to JSON or CSV
    if output_format.lower() == "csv":
        flat_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    else:
        flat_df.coalesce(1).write.mode("overwrite").json(output_path)

    spark.stop()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True, help="Path to input AVRO file")
    parser.add_argument("--schema_path", required=True, help="Path to AVRO schema file (AVSC)")
    parser.add_argument("--output_path", required=True, help="Path to output folder")
    parser.add_argument("--output_format", choices=["json", "csv"], default="json", help="Output format")
    args = parser.parse_args()

    main(args.input_path, args.schema_path, args.output_path, args.output_format)
