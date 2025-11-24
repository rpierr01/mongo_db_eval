# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:21:43 2024

@author: fgarni04
"""
import pymongo
import pandas

connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.resto

try:
    connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    print("Connexion réussie :", connex.list_database_names())
except Exception as e:
    print("Erreur de connexion :", e)

# 1. Donner les styles de cuisine présent dans la collection
print(db.restaurants.distinct("cuisine"))

# 2. Donner tous les grades possibles dans la base
print(db.restaurants.distinct("grades.grade"))

# 3. Compter le nombre de restaurants proposant de la cuisine française (“French”)
print(db.restaurants.count_documents({"cuisine": "French"}))

# 4. Compter le nombre de restaurants situé sur la rue “Central Avenue”
print(db.restaurants.count_documents({"address.street": "Central Avenue"}))

# 5. Compter le nombre de restaurants ayant eu une note supérieure à 50
print(db.restaurants.count_documents({"grades.score": {"$gt": 50}}))

# 6. Lister tous les restaurants, en n’affichant que le nom, l’immeuble et la rue
print(pandas.DataFrame(db.restaurants.find({}, {"name": 1, "address.building": 1, "address.street": 1, "_id": 0})))

# 7. Lister tous les restaurants nommés “Burger King” (adresse rue et quartier uniquement) trié par quartier et rue décroissante
print(pandas.DataFrame(db.restaurants.find({"name": "Burger King"}, 
                                           {"address.street": 1, "borough": 1, "_id": 0}, 
                                           sort = [("borough", 1),("address.street",-1)]
                                           )))


# 8. Lister les restaurants situés sur les rues “Union Street” ou “Union Square”
print(pandas.DataFrame(db.restaurants.find({"address.street": {"$in": ["Union Street", "Union Square"]}})))

# 9. Lister les restaurants situés au-dessus de la latitude 40.90
print(pandas.DataFrame(db.restaurants.find({"address.coord.1": {"$gt": 40.90}})))

# 10. Lister les restaurants ayant eu un score de 0 et un grade “A”
print(pandas.DataFrame(db.restaurants.find({"grades.score": 0, "grades.grade": "A"})))

# 11. Lister les restaurants (nom et rue uniquement) situés sur une rue ayant le terme “Union” dans le nom
print(pandas.DataFrame(db.restaurants.find({"address.street": {"$regex": "Union"}},
                                           {"name": 1, "address.street": 1, "_id": 0}
                                           )))

# 12. Lister les restaurants ayant eu une visite le 1er février 2014
from datetime import datetime
print(pandas.DataFrame(db.restaurants.find({"grades.date": datetime(2014, 2, 1)})))

# 13. Lister les restaurants situés entre les longitudes -74.2 et -74.1 et les latitudes 40.1 et 40.2
print(pandas.DataFrame(db.restaurants.find({
    "address.coord.0": {"$gte": -74.2, "$lte": -74.1},
    "address.coord.1": {"$gte": 40.1, "$lte": 40.2}
})))
