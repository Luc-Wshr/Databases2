# Unzip Zip-Files
import zipfile
import pyspark.sql.functions as F

# specify the zip file name
zip_file_name = "202211-citibike-tripdata.csv.zip"

# create a ZipFile object
zip_file = zipfile.ZipFile(zip_file_name)

# extract all files from the zip file
# the extractall() method will extract all the files to the current directory
zip_file.extractall()

# close the ZipFile object
zip_file.close()
