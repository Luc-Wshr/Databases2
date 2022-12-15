import redis
from csv import reader
import datetime

def redisImportAlmostAll(searchPattern):
    with open('group_membership.tsv', newline='\n', encoding='utf-8') as csvfile:
        linereader = reader(csvfile, delimiter='\t')
        next(linereader)  # skip headings
        counter = 0
        # Redis aufmachen
        r = redis.Redis()

        # searchPattern = int(input("how many instruments? "))
        for row in linereader:
            # dieses Element sind alle Instrumente durch Komma getrennt
            instruments = row[4].split(",")
            # leider gibt split ein leeres Element zurueck, deswegen kann man nicht len(instruments) nutzen
            # also: wenn das erste Split-Element nicht leer ist, dann gehen wir durch die Instrumentenliste
            # und speichern die Key-Value Paare in Redis
            if len(instruments) > searchPattern:
                print(f"len = {len(instruments)} : {row}")
                counter += 1
                for instrument in instruments:
                    redisKey = "artist:" + row[3] + ":" + row[2];
                    redisVal = instrument
                    print(f"rediskey = { redisKey }, redisVal = { redisVal }\n")
                    r.sadd(redisKey, redisVal)

    print(f"The total number of records is { counter }\n")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """
    today = datetime.date.today()
    stoday = today.isoformat()
    r = redis.Redis()
    visitors = {"danny", "jonny", "alexij"}
    r.sadd(stoday, *visitors)
    print(f"noof members: {r.scard(today.isoformat())}")
    r.close()
    """
    redisImportAlmostAll(4)

