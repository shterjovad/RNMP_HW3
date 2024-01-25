from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, DoubleType
from pyspark.sql import functions as F
from pyspark.ml.linalg import DenseVector
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType


# Initialize Spark session for Structured Streaming
spark = SparkSession.builder \
    .appName("KafkaIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Load data stream from Kafka
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("startingOffsets", "earliest") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "health_data") \
    .load()

# Define the schema
schema = StructType([
    StructField('Diabetes_binary', DoubleType(), True),
    StructField('HighBP', DoubleType(), True),
    StructField('HighChol', DoubleType(), True),
    StructField('CholCheck', DoubleType(), True),
    StructField('BMI', DoubleType(), True),
    StructField('Smoker', DoubleType(), True),
    StructField('Stroke', DoubleType(), True),
    StructField('HeartDiseaseorAttack', DoubleType(), True),
    StructField('PhysActivity', DoubleType(), True),
    StructField('Fruits', DoubleType(), True),
    StructField('Veggies', DoubleType(), True),
    StructField('HvyAlcoholConsump', DoubleType(), True),
    StructField('AnyHealthcare', DoubleType(), True),
    StructField('NoDocbcCost', DoubleType(), True),
    StructField('GenHlth', DoubleType(), True),
    StructField('MentHlth', DoubleType(), True),
    StructField('PhysHlth', DoubleType(), True),
    StructField('DiffWalk', DoubleType(), True),
    StructField('Sex', DoubleType(), True),
    StructField('Age', DoubleType(), True),
    StructField('Education', DoubleType(), True),
    StructField('Income', DoubleType(), True)
])

# Assuming the data is in JSON format
# df_stream = df_stream.selectExpr("CAST(value AS STRING)")
df_stream = df_stream.drop(*["Fruits" , "Veggies" , "Sex" , "CholCheck" , "AnyHealthcare"])
df_stream = df_stream.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")

# Load serialized model
model = LogisticRegressionModel.load("model/")
feature_columns = ['HighBP',
 'HighChol',
 'BMI',
 'Smoker',
 'Stroke',
 'HeartDiseaseorAttack',
 'PhysActivity',
 'HvyAlcoholConsump',
 'NoDocbcCost',
 'GenHlth',
 'MentHlth',
 'PhysHlth',
 'DiffWalk',
 'Age',
 'Education',
 'Income']
label_column = 'Diabetes_binary'
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(df_stream).select(col("features"), col(label_column).alias("label"))


# Apply transformations and predictions
predictions = model.transform(data)

# Assuming that the 'features' column is a DenseVector
def to_array(col):
    def to_array_(v):
        return v.toArray().tolist()
    return udf(to_array_, ArrayType(FloatType())).asNondeterministic()(col)

df = predictions.withColumn("features_arr", to_array(col("features")))

feature_columns = ['HighBP', 'HighChol', 'BMI', 'Smoker', 'Stroke', 'HeartDiseaseorAttack', 'PhysActivity', 'HvyAlcoholConsump', 'NoDocbcCost', 'GenHlth', 'MentHlth', 'PhysHlth', 'DiffWalk', 'Age', 'Education', 'Income']

for i, feature in enumerate(feature_columns):
    df = df.withColumn(feature, df["features_arr"].getItem(i))

df = df.drop("features", "features_arr")

# Write predictions to Kafka
query = df \
    .select(F.to_json(F.struct([df[c] for c in df.columns])).alias("value")) \
    .selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "health_data_predicted") \
    .option("checkpointLocation", "./kafka_checkpoints") \
    .start()

query.awaitTermination()

spark.stop()
