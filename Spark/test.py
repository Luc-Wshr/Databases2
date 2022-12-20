# Change string to time in year
from pyspark.sql.types import StringType,BooleanType,DateType,TimestampType,IntegerType,FloatType

datedata = {
    cleandata.withColumn("startYear",F.col("startYear").cast(DateType()))
    .withColumn("endYear",F.col("endYear").cast(DateType()))
    .withColumn("averageRating",F.col("averageRating").cast(FloatType()))
    .withColumn("numVotes",F.col("numVotes").cast(FloatType()))
}
datedata.show()
