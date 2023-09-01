from pyspark.sql import SparkSession
import random

def inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Pi Calculation") \
        .getOrCreate()

    num_samples = 100000
    num_partitions = 10

    count = spark.sparkContext.parallelize(range(0, num_samples), num_partitions).filter(inside).count()
    pi = 4.0 * count / num_samples

    print("Cluster is running as expected.")

    spark.stop()