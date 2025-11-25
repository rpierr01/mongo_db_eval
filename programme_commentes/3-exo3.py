# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 16:03:45 2024

@author: fgarni04
"""


# 1. Donner le nombre de logements

# Importation des bibliothèques nécessaires
import pymongo
import pandas

# Connexion au serveur MongoDB local
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
# Sélection de la base de données airbnb
db = connex.airbnb

# 1. Nb logements
# count_documents({}) compte tous les documents (filtre vide = tous)
print(db.logements.count_documents({}))

# 2. Lister les différentes types de logements possibles cf (room_type)
# distinct() retourne toutes les valeurs uniques du champ room_type
print(db.logements.distinct("room_type"))

# 3. Lister les différents équipements possibles (amenities)
# amenities est un tableau, distinct() retourne tous les équipements uniques
print(db.logements.distinct("amenities"))

# 4. Donner le nombre de logements de type "Entire home/apt"
# Filtre exact sur le champ room_type
print(db.logements.count_documents({"room_type": "Entire home/apt"}))

# 5. Donner le nombre de logements proposant la "TV" et le "Wifi (cf amenities)
# $and : les deux conditions doivent être vraies
# Pour un tableau, la condition vérifie si l'élément est présent
print(db.logements.count_documents({
    "$and": [
        {"amenities": "TV"},      # amenities contient "TV"
        {"amenities": "Wifi"} ]   # ET amenities contient "Wifi"
    }))

# 6. Donner le nombre de logements n'ayant eu aucun avis 
print(db.logements.count_documents({
    "$and": [
       # number_of_reviews égal à 0
       {"number_of_reviews": 0},
       # ET le tableau reviews existe ($exists) et est vide ($eq: [])
       {"reviews": {"$exists": True, "$eq": []}}
    ]}))

# 7. Lister les informations du logement 10545725 (sans _id)
# find_one() retourne un seul document correspondant au filtre
# {"_id": 0} dans la projection masque le champ _id
print(db.logements.find_one({"_id": "10545725"}, {"_id": 0}))

# 8. Lister le nom, la rue et le pays des logements dont le prix est 
# supérieur à 10000 
df = pandas.DataFrame(db.logements.find(
    # $gt : greater than (supérieur à)
    {"price": {"$gt": 10000}},
    # Projection : sélectionne les champs à afficher
    # 1 = afficher, 0 = masquer
    # Notation pointée pour les sous-documents (address.street)
    {"name": 1, "address.street": 1, "address.country": 1, "_id": 0}
))
print(df)


# 9. Donner le nombre de logements par type 
# Pipeline d'agrégation simple
print(pandas.DataFrame(db.logements.aggregate([
    # Regroupe par type de logement et compte
    {"$group": {"_id": "$room_type", "count": {"$sum": 1}}}
])))

# OU en plus trié !
# $sortByCount combine $group + $sum + $sort en une seule étape
print(pandas.DataFrame(db.logements.aggregate([
    {"$sortByCount": "$room_type"}
])))

# 10. Donner le nombre de logements par pays
print(pandas.DataFrame(db.logements.aggregate([
    # Regroupe par pays (sous-document address.country)
    {"$group": {"_id": "$address.country", "count": {"$sum": 1}}}
])))

# OU + trié !
print(pandas.DataFrame(db.logements.aggregate([
    {"$sortByCount": "$address.country"}
])))

# 11. Calculer pour chaque type de logements (room_type) la moyenne des prix (price) 
# "avec arrondi"
df=pandas.DataFrame(db.logements.aggregate([
    {"$group": {
        "_id": "$room_type",
        # $avg calcule la moyenne des valeurs du champ price
        "average_price": {"$avg": "$price"}
    }},
    {"$project": {
        "_id":0,
        # Renommage des champs pour plus de clarté
        "Type de chambre": "$_id", 
        # $round arrondit à 2 décimales
        "Prix moyen": {"$round": ["$average_price", 2]}
    }}
]))

# Attention valeurs numériques issues de requete mondoDB ne donnent pas les bons types
# Correction nécessaire : conversion string -> float
# Remplace les virgules par des points (format européen -> format US)
df["Prix moyen"] = df["Prix moyen"].astype(str).str.replace(",", ".")
# Convertit en numérique, errors="coerce" transforme les erreurs en NaN
df["Prix moyen"] = pandas.to_numeric(df["Prix moyen"], errors="coerce")

# Création d'un graphique à barres avec seaborn
import seaborn as sns
import matplotlib.pyplot as plt
# Définit la taille de la figure
plt.figure(figsize=(10, 6))
# Crée un barplot : x = catégories, y = valeurs numériques
sns.barplot(data=df, x="Type de chambre", y="Prix moyen", palette="viridis")

# Ajout des titres et labels
plt.title("AIRBNB - Prix moyen par type de chambre")
plt.xlabel("Type Chambre")
plt.ylabel("Prix moyen ($)")
# Rotation des labels de l'axe x pour meilleure lisibilité
plt.xticks(rotation=45)
plt.show()



# 12. Pour chaque logement, quel est le nombre d'avis ?
df = pandas.DataFrame(db.logements.find(
    {},  # Pas de filtre = tous les documents
    {"name": 1, 
     "number_of_reviews": 1, 
     "_id": 0, 
     # $size retourne la taille du tableau reviews
     "reviews_count": {"$size": "$reviews"}}))
print(df)

# 13. Compter le nombre de logements pour chaque équipement possible 
print(pandas.DataFrame(db.logements.aggregate([
    # $unwind déplie le tableau amenities : 1 document par équipement
    {"$unwind": "$amenities"},
    # Regroupe par équipement et compte
    {"$group": {"_id": "$amenities", "count": {"$sum": 1}}}
     ])))   

# OU préférable car trié !
# $sortByCount effectue unwind + group + sort automatiquement
print(pandas.DataFrame(db.logements.aggregate([
        {"$unwind": "$amenities"},
        {"$sortByCount": "$amenities"}
         ])))        

# 14. On souhaite connaître les 10 utilisateurs ayant fait le plus de commentaires
df = pandas.DataFrame(db.logements.aggregate([
    # Déplie le tableau reviews
    {"$unwind": "$reviews"},
    {"$group": {
        # Regroupe par utilisateur (id + nom)
        # _id peut être un document avec plusieurs champs
        "_id": {
            "reviewer_id": "$reviews.reviewer_id",
            "reviewer_name": "$reviews.reviewer_name"
        },
        # Compte le nombre de reviews par utilisateur
        "count": {"$sum": 1}
    }},
    # Trie par nombre de commentaires décroissant
    {"$sort": {"count": -1}},
    # Garde seulement les 10 premiers
    {"$limit": 10}
 ]))
print(df)

# Note : impossible avec $sortByCount dans MongoDB car il n'accepte qu'un seul champ 
# ou une seule expression préfixée par $, et ne permet pas de grouper directement 
# plusieurs champs (comme reviewer_id et reviewer_name).
