#Aufgabe 1b:

#Aggregation so anpassen, dass nur Ergebnisse angezeigt werden, welche mit "Englisch" anfangen

from pymongo import MongoClient
import pprint
​
# Replace XXXX with your connection URI from the Atlas UI
# hier sind user und pw "superduper", die beide in der "admin" Datenbank angelegt sind
# (z.B. https://medium.com/mongoaudit/how-to-enable-authentication-on-mongodb-b9e8a924efac)
client = MongoClient("mongodb://superadmin:superadmin@localhost:27017/mflix?authMechanism=DEFAULT", connect=True, authSource="admin")
​
#print(client.mflix)
​
# hier sind die 2 Varianten, wie man auf die Datenbank zugreifen kann>:
# Option 1: (der Dictionary-Ansatz (keine Ahnung, wie der offiziell heisst))
db = client["mflix"]
collection = db["movies_initial"]
​
langPipeng = collection.aggregate([
    { 
        "$match": {
            "language": {"$regex": "^English" }
        } 
    },
    { 
        "$group": {
            "_id": "$language",
            "count": {"$sum": 1},
            #"anzahl": {"$count": {}}
        }
    },
    {
        "$sort": { "count" : -1}
    }
]);
​
​
for x in list(langPipeng):
    pprint.pprint(x)
    
    
 
#{'_id': 'English', 'count': 25325}
#{'_id': 'English, Spanish', 'count': 728}
#{'_id': 'English, French', 'count': 584}
#{'_id': 'English, German', 'count': 288}
#{'_id': 'English, Italian', 'count': 263}
#{'_id': 'English, Russian', 'count': 159}
#{'_id': 'English, Japanese', 'count': 159}
#{'_id': 'English, Latin', 'count': 61}
#{'_id': 'English, Mandarin', 'count': 60}
#{'_id': 'English, French, German', 'count': 55}
#{'_id': 'English, Portuguese', 'count': 53}
