# Aufgabe 1

Nenne 5 **verschiedene** Anwendungen wo Key-Value-Datenbanken Vorteile gegenüber Relationalen Datenbanken haben, und beschreibe spezifisch (aber dennoch stichwortartig) für jeden Fall, was genau der jeweilige Vorteil ist (und: was potentielle Nachteile sind)

# Lösung:

- Hohe Skalierbarkeit: Key-Value-Datenbanken sind in der Lage, sehr große Mengen an Daten effizient zu verarbeiten und zu speichern. Dies macht sie ideal für Anwendungen, die eine hohe Skalierbarkeit erfordern, wie beispielsweise soziale Netzwerke, E-Commerce-Plattformen oder Cloud-Dienste. Der Vorteil gegenüber relationalen Datenbanken besteht darin, dass sie keine komplexen Joins oder Indexe benötigen, was die Performance verbessert.

- Einfache Implementation: Key-Value-Datenbanken haben ein sehr einfaches Datenmodell, das sich leicht verstehen und implementieren lässt. Sie ermöglichen es Entwicklern, schnell und einfach Daten zu speichern und abzufragen, ohne sich um die Definition von Schemata oder die Implementierung von Indexen kümmern zu müssen. Der Vorteil gegenüber relationalen Datenbanken besteht darin, dass sie weniger komplex sind und weniger Zeit und Ressourcen für die Integration und Verwaltung erfordern.

- Hochverfügbarkeit: Key-Value-Datenbanken sind in der Lage, Daten auf mehreren Servern zu replizieren und zu speichern, um eine hohe Verfügbarkeit zu gewährleisten. Dies macht sie ideal für Anwendungen, die eine hohe Verfügbarkeit erfordern, wie beispielsweise Online-Spiele oder Echtzeit-Anwendungen. Der Vorteil gegenüber relationalen Datenbanken besteht darin, dass sie weniger anfällig für Ausfälle sind und eine höhere Verfügbarkeit bieten.

- Echtzeit-Anwendungen: Key-Value-Datenbanken sind in der Lage, schnelle Lesezugriffe auf große Mengen an Daten zu ermöglichen, was sie ideal für Echtzeit-Anwendungen macht. Beispiele hierfür sind Chat-Apps oder Online-Spiele, die eine schnelle Reaktion auf Benutzereingaben erfordern. Der Vorteil gegenüber relationalen Datenbanken besteht darin, dass sie weniger Zeit für die Verarbeitung von Anfragen benötigen, was die Performance verbessert.

- Speicherung von unstrukturierten Daten: Key-Value-Datenbanken sind in der Lage, beliebige Arten von Daten zu speichern, ohne dass sie in einem vordefinierten Schema strukturiert wer

# Aufgabe 2

erkläre was denn nun wirklich die Hauptunterschiede zwischen einem Data Warehouse und einem Data Lake sind und gib Beispiele für Anwendungen in denen entweder ein Data Warehouse oder ein Data Lake die bessere Wahl ist

# Lösung:

Ein Data Warehouse ist ein zentraler Ort, an dem Daten aus verschiedenen Quellen gespeichert und analysiert werden können. Es ist in der Regel strukturiert und optimiert für schnelle Abfragen und Berichterstellung. Ein Data Warehouse wird häufig verwendet, um historische Daten für die Unternehmensplanung und -entscheidungen zu speichern und zu analysieren.

Ein Data Lake ist ein zentraler Ort, an dem große Mengen an unstrukturierten und strukturierten Daten aus verschiedenen Quellen gespeichert werden können. Im Gegensatz zu einem Data Warehouse ist ein Data Lake weniger fokussiert auf die schnelle Abfrage von Daten und mehr auf die Speicherung und Verarbeitung von großen Datenmengen. Ein Data Lake wird häufig verwendet, um Daten aus verschiedenen Quellen zu integrieren und für maschinelles Lernen und andere analytische Zwecke bereitzustellen.

Ein Beispiel für die Verwendung eines Data Warehouses wäre die Erstellung von Berichten und Analysen für das Management, die auf historischen Verkaufsdaten basieren. Ein Beispiel für die Verwendung eines Data Lakes wäre die Integration von Daten aus verschiedenen Quellen, wie Online-Transaktionen, Social-Media-Daten und Sensordaten, um Kundenverhalten und -trends zu analysieren.

Im Allgemeinen ist ein Data Warehouse die bessere Wahl, wenn Sie strukturierte Daten für schnelle Abfragen und Berichterstellung benötigen, während ein Data Lake die bessere Wahl ist, wenn Sie große Mengen an unstrukturierten Daten integrieren und für maschinelles Lernen und andere analytische Zwecke bereitstellen möchten. Es ist jedoch wichtig zu beachten, dass Data Warehouses und Data Lakes nicht vollständig voneinander abhängig sind und dass es möglich ist, sie in einer gemeinsamen Datenplattform zu integrieren, um die Vorteile beider Systeme zu nutzen.

# Aufgabe 3

im folgenden Code hat sich der Fehlerteufel eingeschlichen. Finde die 5 Fehler (10 Punkte), und beschreibe stichwortartig, was der Code denn tun würde, wenn keine Fehler drin wären.

# Lösung:

- The first error is that the connect variable is being assigned the result of a tuple, rather than a dictionary. To fix this, you should remove the square brackets around "collection".
- Der zweite Fehler ist, dass MongoClient nie importiert wurde, und daher auch nicht verwendet werden kann
- Der dritte Fehler ist die unten verwendete connection.aggregate, hier müsste connect.aggregate verwendet werden, da so unsere Verbindung oben benannt wurde
- Der vierte Fehler ist der $mismatch operator, den gibt es nicht, hier muss der $match operator genutzt werden
- Der fünfte Fehler ist, dass die offene Klammer bei "pipe2 = [" niemals geschlossen wurde

If these errors were fixed, the code would:

- Connect to a MongoDB database on localhost at port 27017, and select the "database" and "collection" collections.
- Create a pipeline for an aggregate query that filters documents by the "origin" and "dest" fields, groups the documents by the "origin" and "destination" fields, and calculates the number of "Success" and "Failure" documents (where "Success" is defined as documents where "cancelled" is equal to 0 and "Failure" is defined as documents where "cancelled" is equal to 1). It also calculates the total number of documents in each group and the percentage of failures in each group.
- Sort the results of the aggregate query by the "origin" and "destination" fields in ascending order.
- Execute the aggregate query and assign the results to the result variable.
- Print the result variable.

# Aufgabe 4


# Lösung:
Lazy Evaluation ist eine Technik, bei der die Auswertung eines Ausdrucks so lange aufgeschoben wird, bis sie tatsächlich benötigt wird. Dies kann nützlich sein, um die Leistung eines Programms zu optimieren, indem unnötige Berechnungen vermieden werden, und um die Erstellung von unendlichen Datenstrukturen zu ermöglichen.

- Beispiele dazu kann ich mich nicht mehr erinnern, waren aber glaub einmal dieses "get" auf eine MongoDB wo alles abgerufen wird, und einmal wo nur ein single-value abgefragt wird
