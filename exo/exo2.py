import pymongo
import pandas as pd

# Connexion à la base de données MongoDB
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.resto

# 1. Quelles sont les 10 plus grandes chaines de restaurants (nom identique) ? 
requete = pd.DataFrame(db.restaurants.aggregate([
	{"$group": {"_id": "$name", "nbRestos" : {"$sum":1}}},
	{"$sort": {"nbRestos": -1}},
	{"$limit": 10}
	]))


# 2. Donner le Top 5 et le Flop 5 des types de cuisine, en terme de nombre de restaurants (2 requêtes)
requete = pd.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5},
    {"$project": {"_id":0,"type de cuisine": "$_id", "nbRestos":"$count"}}
    ]))
print(requete)

requete = pd.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": {"count": 1}},
    {"$limit": 5},
    {"$project": {"_id":0,"type de cuisine": "$_id", "nbRestos":"$count"}}
    ]))
print(requete)

# 7. Quels sont les 10 restaurants (nom, quartier, adresse et score) avec le plus petit score moyen ? (ôter les None dans score)

requete = pd.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": {"count": 1}},
    {"$limit": 5},
    {"$project": {"_id":0,"type de cuisine": "$_id", "nbRestos":"$count"}}
    ]))