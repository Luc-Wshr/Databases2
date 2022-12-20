import redis
import datetime 
import numpy as numpy

redis = redis.Redis(host='127.0.0.1', port=6379, db=0)

for anfragen in [100, 5000, 10000, 250000, 500000, 1000000]:
    print(f"gestartet um {datetime.datetime.now()}")

    zeiten = []
    for i in range(anfragen):
        schlüssel = f"{i}"
        wert = "Der Wert"

        anfang = datetime.datetime.now()
        redis.get(schlüssel)
        ende = datetime.datetime.now()

        zeiten.append((ende - anfang).total_seconds())

    zeiten = numpy.array(zeiten)

    print(f"anfragen = {anfragen}")
    print(f"minimum: {numpy.min(zeiten):.4f}s")
    print(f"maximum: {numpy.max(zeiten):.4f}s")
    print(f"durchschnitt: {numpy.average(zeiten):.4f}s")
    print(f"stunden: {numpy.std(zeiten):.4f}s")
    print()