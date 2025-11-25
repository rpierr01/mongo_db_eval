import pymongo
import pandas as pd

# Connexion à la base de données MongoDB
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.airbnb

# 1 donner le nombre de logement
requete = db.logements.count_documents({})
print("1. Nombre de logements : ", requete)
print("-" * 50)

# 2 Lister les différentes types de logements possibles cf (room_type)

requete = db.logements.distinct("room_type")
print("2. les différents room_type : ",requete)
print("-" * 50)

# 3 Lister les différents équipements possibles cf (amenities)
requete = db.logements.distinct("amenities")
print("3. les différents équipements : ",requete)
print("-" * 50)

# 4 Donner le nombre de logements de type “Entire home/apt”
requete = db.logements.count_documents({"room_type": "Entire home/apt"})
print("4. Nombre de Entire home/apt : ",requete)
print("-" * 50)

# 5 Donner le nombre de logements proposant la “TV” et le “Wifi (cf amenities)
requete = db.logements.count_documents({"$and": [{"amenities": "Wifi"}, {"amenities": "TV"}]})
print("5. nombre de logements proposant la “TV” et le “Wifi : ",requete)
print("-" * 50)

# 6 Donner le nombre de logements n’ayant eu aucun avis (champs number_of_reviews et reviews (tableau des avis))
requete = db.logements.count_documents({"number_of_reviews": 0})
print("6. nombre de logements sans avis : ",requete)
print("-" * 50)

# 7 Lister les informations du logement “10545725” (sans _id)
requete = db.logements.find_one(
	{"_id": "10545725"},
	{"_id": 0}
	)

print("7. : ",requete)
print("-" * 50)

# 8 Lister le nom, la rue et le pays des logements dont le prix est supérieur à 10 000.
requete = pd.DataFrame(db.logements.find(
	{"price" : {"$gt": 10000}},
	{"name": 1, "address": 1, "_id": 0}
	))
print(requete)
print("-" * 50)

# 9 Donner le nombre de logements par type
requete = pd.DataFrame(db.logements.aggregate([
	{"$sortByCount": "$room_type"}
	]))
print(requete)
print("-" * 50)

# 10 Donner le nombre de logements par pays
requete = pd.DataFrame(db.logements.aggregate([
	{"$sortByCount": "$address.country"}
	]))
print(requete)
print("-" * 50)

# 11 Calculer pour chaque type de logements (room_type) la moyenne des prix (price) 
df=pd.DataFrame(db.logements.aggregate([
    {"$group": {"_id": "$room_type", "average_price": {"$avg": "$price"}}},
    {"$project": {"_id":0,"Type de chambre": "$_id", 
                  "Prix moyen": {"$round": ["$average_price", 2]}}}
]))
print(df)
print("-" * 50)

# 12 Pour chaque logement, quel est le nombre d’avis ?
requete = pd.DataFrame(db.logements.find(
	{},
	{"name": 1, "_id": 0, "number_of_reviews": 1, "nb_reviews": {"$size": "$reviews"}}
))
print(requete)
print("-" * 50)

