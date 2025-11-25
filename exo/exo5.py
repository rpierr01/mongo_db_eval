import pymongo
import pandas as pd

# Connexion à la base de données MongoDB
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db1 = connex.airbnb
db2 = connex.resto

# 1 Trouve tous les restaurants situés dans le Bronx dont le nom commence par le mot "The" (Ex: "The Bronx Grill"). Affiche uniquement le Nom et le Quartier.

requete = pd.DataFrame(db2.restaurants.find(
    {"borough": "Bronx",
    "name": {"$regex": "^The"}},
    {"name": 1, "_id": 0, "borough": 1}
    ))
    
print("2. : ", requete)
print("-" * 50)

# 2 Quels sont les 5 logements qui proposent le plus d'équipements (amenities) ? Affiche le Nom du logement et le Nombre d'équipements.
requete = pd.DataFrame(db1.logements.aggregate([
    {"$project": {
        "name": 1, 
        "_id": 0,
        "amenities_count": {"$size": "$amenities"}
    }},
    {"$sort": {"amenities_count": -1}},
    {"$limit": 5}
]))
print(requete)

# 3 
requete = pd.DataFrame(db1.logements.aggregate([
    {"$match": {"name": "McDonald's"}}
]))
print(requete)