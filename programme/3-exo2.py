# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:21:43 2024

@author: fgarni04
"""
import pymongo
import pandas

connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.resto

# 1. Quelles sont les 10 plus grandes chaines de restaurants (nom identique) ? 
requete = pandas.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": "$name", "nbRestos": {"$sum": 1}}},
    {"$sort": {"nbRestos": -1}},
    {"$limit": 10},
    {"$project": {"_id":0,"nom de la chaîne de restaurants": "$_id", "nbRestos":1}}
    ]))
print(requete)

# Avec $sortbycount
requete = (pandas.DataFrame(db.restaurants.aggregate([
    {"$sortByCount" : "$name"},
    {"$limit": 10},
    {"$project": {"_id":0,"nom de la chaîne de restaurants": "$_id", "nbRestos":"$count"}}
    ])))
print(requete)

# 2. Donner le Top 5 et le Flop 5 des types de cuisine, en terme de nombre de restaurants 
# TOP 5
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5},
    {"$project": {"_id":0,"type de cuisine": "$_id", "nbRestos":"$count"}}
    ])))

# # Ou $sortByCount
# print(pandas.DataFrame(db.restaurants.aggregate([
#     {"$sortByCount": "$cuisine"},
#     {"$limit": 5},
#     {"$project": {"_id":0,"type de cuisine": "$_id", "nbRestos":"$count"}}
#     ])))

# FLOP 5
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": {"count": 1}},
    {"$limit": 5},
    {"$project": {"_id":0,"type de cuisine": "$_id", "nbRestos":"$count"}}
    ])))

# 3. Quelles sont les 10 rues avec le plus de restaurants ? 
df = pandas.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": "$address.street", "nbRestos": {"$sum": 1}}},
    {"$sort": {"nbRestos": -1}},
    {"$limit": 10},
    {"$project": {"_id":0,"Rue New York": "$_id", "nbRestos":1}}
    ]))
print(df)

# Ou $sortByCount
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$sortByCount": "$address.street"},
    {"$limit": 10},
    {"$project": {"_id":0,"Rue New York": "$_id", "nbRestos":"$count"}}
    ])))

# 4. Quelles sont les rues situées sur strictement 2 quartiers ? 
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": "$address.street", "quartiers": {"$addToSet": "$borough"}}},
    {"$match": {"quartiers": {"$size": 2}}}, 
    {"$project": {"Rue": "$_id", "Quartiers": "$quartiers", "_id": 0}}
    ])))

# 5. Lister par quartier le nombre de restaurants et le score moyen 
# (Attention à bien découper le tableau grades) 

print(pandas.DataFrame(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$group": {
        "_id": "$borough",
        "nb restaurants": {"$sum": 1},
        "score moyen": {"$avg": "$grades.score"}
    }}
    ])))

# avec round
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$group": {
        "_id": "$borough",
        "nb restaurants": {"$sum": 1},
        "score moyen": {"$avg": "$grades.score"}
    }},
    {"$project": {
        "_id": 0,
        "quartier": "$_id",
        "nb restaurants": 1,
        "score moyen": {"$round": ["$score moyen", 2]}
    }}
])))

# ne fonctionne pas car la somme du nb de restos > au nb resto -> pb vient du découpage !
# et en plus moyenne brut -> somme des socres / taille il faudrait par avg par restaurant !
# BONNE REQUETE !
print(pandas.DataFrame(db.restaurants.aggregate([
    # 1) explode les grades 
    {"$unwind": "$grades"},

    # 2) pour chaque restaurant, calculer la moyenne de ses grades
    {"$group": {
        "_id": "$_id",                            # un document = un restaurant
        "borough": {"$first": "$borough"},        # récupérer le borough du restaurant
        "avg_score_per_restaurant": {"$avg": "$grades.score"}
    }},

    # 3) regrouper par borough : compter les restaurants et faire la moyenne des moyennes
    {"$group": {
        "_id": "$borough",
        "nb_restaurants": {"$sum": 1},            # chaque _id ici est un restaurant unique
        "score_moyen": {"$avg": "$avg_score_per_restaurant"}
    }},

    # 4) affichage
    {"$project": {
        "_id": 0,
        "quartier": "$_id",
        "nb restaurants": "$nb_restaurants",
        "score moyen": {"$round": ["$score_moyen", 2]}
    }}
])))


# 6. Donner les dates de début et de fin des évaluations
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$group": {
        "_id": None,
        "Début des évaluations": {"$min": "$grades.date"},
        "Fin des évaluation": {"$max": "$grades.date"}
    }}
    ])))

# 7. Quels sont les 10 restaurants (nom, quartier, adresse et score) 
# avec le plus petit score moyen ?
df = pandas.DataFrame(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$match": {"grades.score": {"$ne": None}}},
    {"$group": {
        "_id": {"nom restaurant": "$name", "quartier": "$borough", "adresse": "$address"},
        "score moyen": {"$avg": "$grades.score"}
    }},
    {"$sort": {"score moyen": 1}},
    {"$limit": 10}
    ]))
print(df)

# 8. Quels sont les restaurants (nom, quartier et adresse rue)
# avec uniquement des grades “A” ? 
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$group": {
        "_id": {"restaurant_id": "$restaurant_id", "nom": "$name", "quartier": "$borough", "adresse": "$address.street"},
        "LesGrades": {"$addToSet": "$grades.grade"}
    }},
    {"$match": {
        "$and": [
            {"LesGrades": {"$size": 1}}, #pas obligatoire mais pour montrer $and
            {"LesGrades": {"$eq": ["A"]}}
        ]
    }},
    {"$project": {
        "nom": "$_id.nom",
        "Quartier": "$_id.quartier",
        "Adresse": "$_id.addresse",
        "_id": 0
    }}
])))

# 9. Compter le nombre d’évaluation par jour de la semaine  
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$project": {"dayOfWeek": {"$dayOfWeek": "$grades.date"}}},
    {"$group": {"_id": "$dayOfWeek", "nb evaluations": {"$sum": 1}}},
    {"$sort": {"nb evaluations": -1}},
])))

# 10. Donner les 3 types de cuisine les plus présents par quartier
df = pandas.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": {"quartier": "$borough", "cuisine": "$cuisine"}, "count": {"$sum": 1}}},
    {"$sort": {"_id.quartier": 1, "count": -1}},
    {"$group": {
        "_id": "$_id.quartier",
        "top_cuisines": {"$push": {"cuisine": "$_id.cuisine", "nb": "$count"}}
    }},
    {"$project": {"top_cuisines": {"$slice": ["$top_cuisines", 3]}}}
]))
