# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 16:03:45 2024

@author: fgarni04
"""


# 1. Donner le nombre de logements

import pymongo
import pandas

connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.airbnb

# 1. Nb logements
print(db.logements.count_documents({}))

# 2. Lister les différentes types de logements possibles cf (room_type)
print(db.logements.distinct("room_type"))

# 3. Lister les différents équipements possibles (amenities)
print(db.logements.distinct("amenities"))

# 4. Donner le nombre de logements de type “Entire home/apt”
print(db.logements.count_documents({"room_type": "Entire home/apt"}))

# 5. Donner le nombre de logements proposant la “TV” et le “Wifi (cf amenities)
print(db.logements.count_documents({
    "$and": [
        {"amenities": "TV"},
        {"amenities": "Wifi"} ]}
    ))

# 6. Donner le nombre de logements n’ayant eu aucun avis 
print(db.logements.count_documents({
    "$and": [
       {"number_of_reviews": 0},
       {"reviews": {"$exists": True, "$eq": []}}
    ]}))

# 7. Lister les informations du logement 10545725 (sans _id)
print(db.logements.find_one({"_id": "10545725"}, {"_id": 0}))

# 8. Lister le nom, la rue et le pays des logements dont le prix est 
# supérieur à 10000 
df = pandas.DataFrame(db.logements.find(
    {"price": {"$gt": 10000}},
    {"name": 1, "address.street": 1, "address.country": 1, "_id": 0}
))
print(df)


# 9. Donner le nombre de logements par type 
print(pandas.DataFrame(db.logements.aggregate([
    {"$group": {"_id": "$room_type", "count": {"$sum": 1}}}
])))

# OU en plus trié !
print(pandas.DataFrame(db.logements.aggregate([
    {"$sortByCount": "$room_type"}
])))

# 10. Donner le nombre de logements par pays
print(pandas.DataFrame(db.logements.aggregate([
    {"$group": {"_id": "$address.country", "count": {"$sum": 1}}}
])))

# OU + trié !
print(pandas.DataFrame(db.logements.aggregate([
    {"$sortByCount": "$address.country"}
])))

# 11. Calculer pour chaque type de logements (room_type) la moyenne des prix (price) 
# "avec arrondi"
df=pandas.DataFrame(db.logements.aggregate([
    {"$group": {"_id": "$room_type", "average_price": {"$avg": "$price"}}},
    {"$project": {"_id":0,"Type de chambre": "$_id", 
                  "Prix moyen": {"$round": ["$average_price", 2]}}}
]))

# Attention valeurs numériques issues de requete mondoDB ne donnent pas les bons types
# Obligé de remplacer les "," par des "." puis transtyper colonne moyenne en numérique
df["Prix moyen"] = df["Prix moyen"].astype(str).str.replace(",", ".")
df["Prix moyen"] = pandas.to_numeric(df["Prix moyen"], errors="coerce")
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



# 12. Pour chaque logement, quel est le nombre d’avis ?
df = pandas.DataFrame(db.logements.find(
    {}, 
    {"name": 1, 
     "number_of_reviews": 1, 
     "_id": 0, 
     "reviews_count": {"$size": "$reviews"}}))
print(df)

# 13. Compter le nombre de logements pour chaque équipement possible 
print(pandas.DataFrame(db.logements.aggregate([
    {"$unwind": "$amenities"},
    {"$group": {"_id": "$amenities", "count": {"$sum": 1}}}
     ])))   

# OU préférable car trié !
print(pandas.DataFrame(db.logements.aggregate([
        {"$unwind": "$amenities"},
        {"$sortByCount": "$amenities"}
         ])))        

# 14. On souhaite connaître les 10 utilisateurs ayant fait le plus de commentaires
df = pandas.DataFrame(db.logements.aggregate([
    {"$unwind": "$reviews"},
    {"$group": {"_id": {
        "reviewer_id": "$reviews.reviewer_id",
        "reviewer_name": "$reviews.reviewer_name"
    }, "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
 ]))
print(df)

# impossible avec $sortByCount dans MongoDB car il n'accepte qu'un seul champ 
# ou une seule expression préfixée par $, et ne permet pas de grouper directement 
# plusieurs champs (comme reviewer_id et reviewer_name).
