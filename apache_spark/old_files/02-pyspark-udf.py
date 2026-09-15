
# UDF in Spark

# Understanding UDF in PySpark
# In PySpark, a User Defined Function (UDF) is a custom function written in Python (or other supported languages) that
# can be applied to DataFrame columns. It allows users to perform transformations and computations that are not
# natively supported by Spark's built-in functions.
#
# UDFs bridge the gap between Spark's optimized, distributed engine and user-defined logic. However, UDFs often come
# with performance trade-offs because they operate outside Spark's optimization framework.
#
# Key Points About UDFs:
# UDFs are used for column-wise transformations.
# The UDF logic is applied row by row, and UDFs run on Spark's worker nodes.
# UDFs are slower than Spark SQL or DataFrame built-in functions since they don’t take
# advantage of Spark's Catalyst optimizer.



# lab

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("UDF Lab - Validate IP Address") \
    .getOrCreate()


# Step 2: Define the Python function
def is_valid_ip(ip_str):
    """
    Function to check if the given IP address string is valid or not.
    """
    try:
        # Split the string by '.' to get parts of the IP address
        parts = ip_str.split('.')

        # Check if there are exactly 4 parts
        if len(parts) != 4:
            return False

        # Check each part is a valid number between 0 and 255
        for part in parts:
            if not part.isdigit():  # Ensure each part is a number
                return False
            if not (0 <= int(part) <= 255):  # Ensure each number is in the range 0-255
                return False

        return True  # IP is valid
    except Exception:
        return False


# Step 3: Register the UDF
is_valid_ip_udf = udf(is_valid_ip, BooleanType())

# Step 4: Create Sample Data
data = [
    ("192.168.1.1",),
    ("10.0.0.256",),
    ("172.16.300.1",),
    ("abc.def.ghi.jkl",),
    ("255.255.255.255",),
    ("0.0.0.0",),
    ("123.045.067.089",),
]

columns = ["ip_address"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Step 5: Apply the UDF to the DataFrame
result_df = df.withColumn("is_valid", is_valid_ip_udf(col("ip_address")))

# Show the Results
result_df.show(truncate=False)
