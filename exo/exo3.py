import pymongo
import pandas as pd

# Connexion à la base de données MongoDB
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.airbnb

# 1 Donner le nombre de logements
print("1. nb de logements : ", db.logements.count_documents({}))
print("-" * 50)

# 2 Lister les différentes types de logements possibles cf (room_type)
print("2. Les différents types de logements sont : ", db.logements.distinct("room_type"))
print("-" * 50)

# 3 Lister les différents équipements possibles cf (amenities)
print("3. les différents équipements possibles sont : ", db.logements.distinct("amenities"))
print("-" * 50)

# 4 Donner le nombre de logements de type “Entire home/apt”
print("4. Nombre de logements de type 'Entire home/apt' : ", db.logements.count_documents({"room_type": "Entire home/apt"}))
print("-" * 50)

# 5 Donner le nombre de logements proposant la “TV” et le “Wifi (cf amenities)
print("5. nombre de logements proposant la “TV” et le “Wifi : ", db.logements.count_documents({"amenities": {"$in": ["TV", "Wifi"]}}))
print("-" * 50)

# 6 Donner le nombre de logements n’ayant eu aucun avis (champs number_of_reviews et reviews (tableau des avis))
print("6. nombre de logements n’ayant eu aucun avis : ", db.logements.count_documents({"number_of_reviews": {"$eq": 0}}))
print("-" * 50)

# 7 Lister les informations du logement “10545725” (sans _id)
print("7. informations du logement “10545725” : ", db.logements.find_one({"_id": "10545725"}, {"_id": 0}))
print("-" * 50)

# 8 Lister le nom, la rue et le pays des logements dont le prix est supérieur à 10 000.
df = pd.DataFrame(db.logements.find(
    {"price": {"$gt": 10000}},
    {"name": 1, "address.street": 1, "address.country": 1, "_id": 0}
))
print("8. nom, rue et pays des logements dont le prix est supérieur à 10 000 : ", df)
print("-" * 50)

# 9 Donner le nombre de logements par type
df = pd.DataFrame(db.logements.aggregate([{"$sortByCount": "$room_type"}]))
print("9. nombre de logements par type : ", df)
print("-" * 50)

# 10 Donner le nombre de logements par pays
df = pd.DataFrame(db.logements.aggregate([{"$sortByCount": "$address.country"}]))
print("10. nombre de logements par pays : ", df)
print("-" * 50)

# 11 Calculer pour chaque type de logements (room_type) la moyenne des prix (price) avec arrondi à 2 décimales et un graphique associé.

df = pd.DataFrame(db.logements.aggregate([
    {"$group": {"_id": "$room_type", "average_price": {"$avg": "$price"}}},
    {"$project": {"_id":0,"Type de chambre": "$_id", 
                  "Prix moyen": {"$round": ["$average_price", 2]}}}
]))

print(df)

df["Prix moyen"] = df["Prix moyen"].astype(str).str.replace(",", ".")
df["Prix moyen"] = pd.to_numeric(df["Prix moyen"], errors="coerce")

import seaborn as sns
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
sns.barplot(data=df, x="Type de chambre", y="Prix moyen", palette="viridis")

# Ajout des titres et labels
plt.title("AIRBNB - Prix moyen par type de chambre")
plt.xlabel("Type Chambre")
plt.ylabel("Prix moyen ($)")
plt.xticks(rotation=45)
plt.show()

print("-" * 50)

# 12 nombre d'avis par logement
df = pd.DataFrame(db.logements.find(
    {}, 
    {"name": 1, 
     "number_of_reviews": 1, 
     "_id": 0, 
     "reviews_count": {"$size": "$reviews"}}))
print(df)
print("-" * 50)

# 13 Compter le nombre de logements pour chaque équipement possible
print(pd.DataFrame(db.logements.aggregate([
        {"$unwind": "$amenities"},
        {"$sortByCount": "$amenities"}
         ]))) 
print("-" * 50)

# 14 On souhaite connaître les 10 utilisateurs (id et nom) ayant fait le plus de commentaires
df = pd.DataFrame(db.logements.aggregate([
    {"$unwind": "$reviews"},
    {"$group": {"_id": {
        "reviewer_id": "$reviews.reviewer_id",
        "reviewer_name": "$reviews.reviewer_name"
    }, "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
 ]))
print(df)
print("-" * 50)

# ------- exo de Gemini --------
# 15 Combien y a-t-il de logements qui sont de type "Private room" ET qui possèdent l'équipement "Pool" (Piscine) ?
print("2. Les différents types de logements sont : ", db.logements.count_documents(
    {"amenities": "TV", }))
print("-" * 50)