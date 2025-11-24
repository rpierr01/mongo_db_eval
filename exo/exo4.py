import pymongo
import pandas as pd

# Connexion à la base de données MongoDB
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.airbnb

# ------- exo de Gemini --------
# 1 Combien y a-t-il de logements qui sont de type "Private room" ET qui possèdent l'équipement "Pool" (Piscine) ?
print("1. logements qui sont de type Private room ET qui possèdent l'équipement Pool : ", db.logements.count_documents(
    {"room_type": "Private room", "amenities": "Pool"}))
print("-" * 50)

# 2 Affiche le nom et le prix des 3 logements les plus chers situés au Brésil (address.country = "Brazil").          
requete = pd.DataFrame(db.logements.find(
    {"address.country": "Brazil"},
    {"name": 1, "_id": 0, "price": 1}
    ).sort("price", -1).limit(3))
    
print("2. nom et prix des 3 logements les plus chers du brésil : ", requete)
print("-" * 50)

# 3 Pour chaque pays (address.country), quel est le prix du logement le moins cher et celui du plus cher ?
df=pd.DataFrame(db.logements.aggregate([
    {"$group": 
    {"_id": "$address.country", 
    "max_price": {"$max": "$price"},
    "min_price": {"$min": "$price"}
    }}]))
print("3. Prix max et min pour chaque pays", df)
print("-" * 50)

# 4 Quels sont les 5 équipements (amenities) les plus fréquents, mais uniquement pour les logements situés aux États-Unis ("United States") ?
df = pd.DataFrame(db.logements.aggregate([
    {"$match" : {"address.country": "United States"}},
    {"$unwind": "$amenities"},
    {"$sortByCount": "$amenities"},
    {"$limit": 5}
    ]))
print("4. top 5 des équipements les plus nombreux aux etats unis : ", df)
print("-" * 50)

db = connex.resto

# 5 Combien y a-t-il de restaurants dans le quartier "Queens" qui ne font PAS de cuisine "American" ?
print("5. restaurants dans le quartier Queens qui ne font PAS de cuisine American  : ", db.restaurants.count_documents(
    {"borough": "Queens", "cuisine": {"$ne": "American"}}))
print("-" * 50)

# 6 Les six logements les moins cher du portugal
db = connex.airbnb
df = pd.DataFrame(db.logements.find(
    {"address.country": "Portugal"},
    {"name": 1, "_id": 0, "price": 1}
    ).sort("price", 1).limit(6))
    
print("6. Les 6 logements les moins chers du portugal : ", df)
print("-" * 50)

# 7 Combien y a-t-il de restaurants pour chaque type de cuisine (cuisine), mais uniquement pour le quartier "Bronx" ?
db = connex.resto
df = pd.DataFrame(db.restaurants.aggregate([
    {"$match" : {"borough": "Bronx"}},
    {"$sortByCount" : "$cuisine"},
    {"$limit": 10}
]))
    
print(df)
print("-" * 50)
# 8 Dans le quartier de "Manhattan", combien de fois la note "C" (la pire) a-t-elle été attribuée ?
db = connex.resto
df = pd.DataFrame(list(db.restaurants.aggregate([
    {"$match" : {"borough": "Manhattan"}},
    {"$unwind": "$grades"},
    {"$match": {"grades.grade": "C"}},
    {"$count" : "Total_notes_C"}
])))

print(df)
print("-" * 50)

# 9. Combien de fois la note "C" a-t-elle été attribuée (Total Global) ?
df_par_quartier = pd.DataFrame(list(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    
    {"$match": {"grades.grade": "C"}},
    # On regroupe par quartier et on compte 1 par note trouvée
    {"$group": {"_id": "$borough", "Total_notes_C": {"$sum": 1}}},
    # Optionnel : On trie pour voir les pires quartiers en premier
    {"$sort": {"Total_notes_C": -1}}
])))
print(df_par_quartier)
print("-" * 50)
# 10. Combien d'évaluations ont été effectuées durant l'année 2013 pour les restaurants de cuisine "Bakery" ?
from datetime import datetime
db = connex.resto
df = db.restaurants.count_documents({
    "cuisine": "Bakery",
    "grades.date": {
        "$gte": datetime(2013, 1, 1),
        "$lt": datetime(2014, 1, 1)
    }
})

print("10 : ", df)