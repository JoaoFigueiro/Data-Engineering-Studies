from pyspark.sql import Row, SparkSession

blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
["twitter", "LinkedIn"])

print(blog_row[1])

spark = (SparkSession
    .builder
    .appName('Test')
    .getOrCreate()
)
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()