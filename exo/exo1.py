import pymongo
import pandas

# Connexion à la base de données MongoDB
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.resto

# 1 Donner les styles de cuisine présents dans la base de données
print(db.restaurants.distinct("cuisine"))

# 2 Donner tous les grades possibles dans la base
print(db.restaurants.distinct("grades.grade"))

# 3 Donner le nombre de restaurants proposant de la cuisine française 'French'
print(db.restaurants.count_documents({"cuisine": "French"}))

