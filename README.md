# avro-flatten
A PySpark-based system that reads nested Avro files, flattens the data, and writes it to JSON or CSV format. The pipeline also integrates Apache Airflow for orchestration.


note: confirm spark version and add avro package accordingly

spark-submit --jars /path/to/spark-avro_2.12-3.5.2.jar \
flatten_avro_to_csv.py \
--input_path /path/to/input.avro \
--schema_path /path/to/schema.avsc \
--output_path /path/to/output/ \
--output_format csv
