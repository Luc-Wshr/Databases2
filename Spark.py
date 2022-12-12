from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("Name der DB") \
      .getOrCreate() 

# Einlesen von Dateien in Spark, Datei Typ einfach Anpassen
test=spark.read.text("test.txt")

# Schema anzeigen lassen
test.printSchema()
#und wenn wir auf das Schema direkt zugreifen wollen, dann gibt es alternativ die dtypes
print(test.dtypes)
#Überprüfen ob ein Element z.B. ein String ist
print(f"Ist das Element == 'string': { test.dtypes[0][1] == 'string'}")

# Daten anzeigen lassen
test.show(30,truncate=False)

# jetzt die Zeilen in einzelne Worte splitten
from pyspark.sql.functions import split
# select selektiert eine oder mehrere Spalten, hier eben nur die 1 Spalte "value"
# alias gibt dem Ergebnis (der selektierten Spalte) einen Namen (sonst waere es eher unhandlich)
# die Funktion split nimmt uebrigens eine REGEXP ... hier ist sie halt super-einfach> ein space
lines = test.select(split(test.value, " ").alias("Zeile"))
lines.show(10, truncate=100)
# das Schema: das Split hat ein Array generiert
lines.printSchema()



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



# CSV Daten auslesen

citybike = spark.read.csv("202211-citibike-tripdata.csv",header = True)
citybike.show(truncate=False)
