#TSV als CSV einlesen
csv_file1 = spark.read.option("header", True).csv(path, sep='\t', encoding='utf8')
