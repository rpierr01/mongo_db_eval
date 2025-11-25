# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:21:43 2024

@author: fgarni04
"""
# Importation de la bibliothèque pymongo pour se connecter à MongoDB
import pymongo
# Importation de pandas pour manipuler les données sous forme de DataFrame
import pandas

# Connexion au serveur MongoDB local sur le port par défaut 27017
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
# Sélection de la base de données 'resto'
db = connex.resto

try:
    # Tentative de connexion au serveur MongoDB
    connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    # Affichage de la liste des bases de données disponibles pour vérifier la connexion
    print("Connexion réussie :", connex.list_database_names())
except Exception as e:
    # En cas d'erreur, affichage du message d'erreur
    print("Erreur de connexion :", e)

# 1. Donner les styles de cuisine présent dans la collection
# distinct() retourne toutes les valeurs uniques du champ "cuisine"
print(db.restaurants.distinct("cuisine"))

# 2. Donner tous les grades possibles dans la base
# Utilisation de la notation pointée pour accéder au champ "grade" dans le tableau "grades"
print(db.restaurants.distinct("grades.grade"))

# 3. Compter le nombre de restaurants proposant de la cuisine française ("French")
# count_documents() compte les documents qui correspondent au filtre
# {"cuisine": "French"} filtre uniquement les restaurants de cuisine française
print(db.restaurants.count_documents({"cuisine": "French"}))

# 4. Compter le nombre de restaurants situé sur la rue "Central Avenue"
# Notation pointée pour accéder au sous-document address et son champ street
print(db.restaurants.count_documents({"address.street": "Central Avenue"}))

# 5. Compter le nombre de restaurants ayant eu une note supérieure à 50
# $gt (greater than) est un opérateur de comparaison MongoDB
# Vérifie si au moins un élément du tableau grades a un score > 50
print(db.restaurants.count_documents({"grades.score": {"$gt": 50}}))

# 6. Lister tous les restaurants, en n'affichant que le nom, l'immeuble et la rue
# find() avec deux paramètres : {} (tous les documents) et la projection
# La projection spécifie les champs à afficher : 1 = afficher, 0 = masquer
# "_id": 0 permet de ne pas afficher l'identifiant MongoDB
print(pandas.DataFrame(db.restaurants.find({}, {"name": 1, "address.building": 1, "address.street": 1, "_id": 0})))

# 7. Lister tous les restaurants nommés "Burger King" (adresse rue et quartier uniquement) trié par quartier et rue décroissante
# sort prend une liste de tuples (champ, ordre) : 1 = croissant, -1 = décroissant
print(pandas.DataFrame(db.restaurants.find({"name": "Burger King"}, 
                                           {"address.street": 1, "borough": 1, "_id": 0}, 
                                           sort = [("borough", 1),("address.street",-1)]
                                           )))


# 8. Lister les restaurants situés sur les rues "Union Street" ou "Union Square"
# $in permet de vérifier si la valeur est dans une liste de valeurs possibles
print(pandas.DataFrame(db.restaurants.find({"address.street": {"$in": ["Union Street", "Union Square"]}})))

# 9. Lister les restaurants situés au-dessus de la latitude 40.90
# address.coord est un tableau [longitude, latitude]
# .1 accède au deuxième élément (index 1) = latitude
print(pandas.DataFrame(db.restaurants.find({"address.coord.1": {"$gt": 40.90}})))

# 10. Lister les restaurants ayant eu un score de 0 et un grade "A"
# Filtre sur deux conditions dans le tableau grades (recherche si au moins un élément correspond)
print(pandas.DataFrame(db.restaurants.find({"grades.score": 0, "grades.grade": "A"})))

# 11. Lister les restaurants (nom et rue uniquement) situés sur une rue ayant le terme "Union" dans le nom
# $regex permet de faire une recherche par expression régulière
# Cherche toutes les rues contenant le mot "Union"
print(pandas.DataFrame(db.restaurants.find({"address.street": {"$regex": "Union"}},
                                           {"name": 1, "address.street": 1, "_id": 0}
                                           )))

# 12. Lister les restaurants ayant eu une visite le 1er février 2014
# Importation de datetime pour créer un objet date
from datetime import datetime
# datetime(2014, 2, 1) crée une date exacte pour la comparaison
print(pandas.DataFrame(db.restaurants.find({"grades.date": datetime(2014, 2, 1)})))

# 13. Lister les restaurants situés entre les longitudes -74.2 et -74.1 et les latitudes 40.1 et 40.2
# $gte = greater than or equal (supérieur ou égal)
# $lte = less than or equal (inférieur ou égal)
# coord.0 = longitude, coord.1 = latitude
print(pandas.DataFrame(db.restaurants.find({
    "address.coord.0": {"$gte": -74.2, "$lte": -74.1},
    "address.coord.1": {"$gte": 40.1, "$lte": 40.2}
})))
