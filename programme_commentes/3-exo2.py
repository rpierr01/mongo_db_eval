# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:21:43 2024

@author: fgarni04
"""
# Importation des bibliothèques nécessaires
import pymongo
import pandas

# Connexion au serveur MongoDB local
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
# Sélection de la base de données 'resto'
db = connex.resto

# 1. Quelles sont les 10 plus grandes chaines de restaurants (nom identique) ? 
# aggregate() permet d'effectuer des opérations complexes en pipeline
requete = pandas.DataFrame(db.restaurants.aggregate([
    # $group : regroupe les documents par nom de restaurant
    # _id définit la clé de regroupement
    # $sum: 1 compte le nombre de documents dans chaque groupe
    {"$group": {"_id": "$name", "nbRestos": {"$sum": 1}}},
    # $sort : trie les résultats par nbRestos de façon décroissante (-1)
    {"$sort": {"nbRestos": -1}},
    # $limit : ne garde que les 10 premiers résultats
    {"$limit": 10},
    # $project : reformate les champs de sortie (renommage et sélection)
    # _id:0 masque le champ _id, "$_id" récupère la valeur de _id dans le champ nom
    {"$project": {"_id":0,"nom de la chaîne de restaurants": "$_id", "nbRestos":1}}
    ]))
print(requete)

# Avec $sortbycount : raccourci combinant $group et $sort
requete = (pandas.DataFrame(db.restaurants.aggregate([
    # $sortByCount : groupe par le champ et trie automatiquement par ordre décroissant
    {"$sortByCount" : "$name"},
    {"$limit": 10},
    # Renommage des champs pour plus de clarté
    {"$project": {"_id":0,"nom de la chaîne de restaurants": "$_id", "nbRestos":"$count"}}
    ])))
print(requete)

# 2. Donner le Top 5 et le Flop 5 des types de cuisine, en terme de nombre de restaurants 
# TOP 5
# Même logique que la requête précédente, mais sur le champ cuisine
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},  # Tri décroissant pour le TOP
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
    {"$sort": {"count": 1}},  # Tri croissant (1) pour le FLOP
    {"$limit": 5},
    {"$project": {"_id":0,"type de cuisine": "$_id", "nbRestos":"$count"}}
    ])))

# 3. Quelles sont les 10 rues avec le plus de restaurants ? 
df = pandas.DataFrame(db.restaurants.aggregate([
    # Regroupement par rue (sous-document address.street)
    {"$group": {"_id": "$address.street", "nbRestos": {"$sum": 1}}},
    {"$sort": {"nbRestos": -1}},
    {"$limit": 10},
    {"$project": {"_id":0,"Rue New York": "$_id", "nbRestos":1}}
    ]))
print(df)

# Ou $sortByCount : version plus courte
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$sortByCount": "$address.street"},
    {"$limit": 10},
    {"$project": {"_id":0,"Rue New York": "$_id", "nbRestos":"$count"}}
    ])))

# 4. Quelles sont les rues situées sur strictement 2 quartiers ? 
print(pandas.DataFrame(db.restaurants.aggregate([
    # $addToSet crée un tableau contenant les valeurs uniques (pas de doublons)
    {"$group": {"_id": "$address.street", "quartiers": {"$addToSet": "$borough"}}},
    # $match filtre les résultats : $size vérifie que le tableau a exactement 2 éléments
    {"$match": {"quartiers": {"$size": 2}}}, 
    {"$project": {"Rue": "$_id", "Quartiers": "$quartiers", "_id": 0}}
    ])))

# 5. Lister par quartier le nombre de restaurants et le score moyen 
# (Attention à bien découper le tableau grades) 

print(pandas.DataFrame(db.restaurants.aggregate([
    # $unwind "déplie" le tableau grades : crée un document par élément du tableau
    {"$unwind": "$grades"},
    {"$group": {
        "_id": "$borough",
        # Compte le nombre de documents (= nombre de notes, pas de restaurants)
        "nb restaurants": {"$sum": 1},
        # $avg calcule la moyenne des scores
        "score moyen": {"$avg": "$grades.score"}
    }}
    ])))

# avec round : arrondit le score moyen à 2 décimales
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
        # $round prend [valeur, nombre de décimales]
        "score moyen": {"$round": ["$score moyen", 2]}
    }}
])))

# PROBLÈME : ne fonctionne pas correctement car après $unwind, on a un document par grade
# donc nb restaurants = somme de tous les grades, pas le nombre de restaurants uniques
# BONNE REQUETE ci-dessous !
print(pandas.DataFrame(db.restaurants.aggregate([
    # 1) explode les grades : crée un document par grade
    {"$unwind": "$grades"},

    # 2) pour chaque restaurant, calculer la moyenne de ses grades
    {"$group": {
        "_id": "$_id",  # Regroupe par identifiant de restaurant
        "borough": {"$first": "$borough"},  # $first récupère la première valeur (toutes identiques)
        "avg_score_per_restaurant": {"$avg": "$grades.score"}  # Moyenne des scores par restaurant
    }},

    # 3) regrouper par borough : compter les restaurants et faire la moyenne des moyennes
    {"$group": {
        "_id": "$borough",
        "nb_restaurants": {"$sum": 1},  # Maintenant chaque _id est un restaurant unique
        "score_moyen": {"$avg": "$avg_score_per_restaurant"}  # Moyenne des moyennes
    }},

    # 4) affichage avec formatage
    {"$project": {
        "_id": 0,
        "quartier": "$_id",
        "nb restaurants": "$nb_restaurants",
        "score moyen": {"$round": ["$score_moyen", 2]}
    }}
])))


# 6. Donner les dates de début et de fin des évaluations
print(pandas.DataFrame(db.restaurants.aggregate([
    # Déplie le tableau grades pour accéder aux dates
    {"$unwind": "$grades"},
    {"$group": {
        "_id": None,  # None = un seul groupe pour tous les documents
        # $min trouve la date minimale (la plus ancienne)
        "Début des évaluations": {"$min": "$grades.date"},
        # $max trouve la date maximale (la plus récente)
        "Fin des évaluation": {"$max": "$grades.date"}
    }}
    ])))

# 7. Quels sont les 10 restaurants (nom, quartier, adresse et score) 
# avec le plus petit score moyen ?
df = pandas.DataFrame(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    # $match filtre les documents : $ne = not equal (différent de)
    # Exclut les scores null/manquants
    {"$match": {"grades.score": {"$ne": None}}},
    {"$group": {
        # Regroupe par plusieurs champs en créant un sous-document
        "_id": {"nom restaurant": "$name", "quartier": "$borough", "adresse": "$address"},
        "score moyen": {"$avg": "$grades.score"}
    }},
    # Tri croissant (1) pour avoir les plus petits scores en premier
    {"$sort": {"score moyen": 1}},
    {"$limit": 10}
    ]))
print(df)

# 8. Quels sont les restaurants (nom, quartier et adresse rue)
# avec uniquement des grades "A" ? 
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$group": {
        # Regroupe par restaurant (identifiant unique + infos)
        "_id": {"restaurant_id": "$restaurant_id", "nom": "$name", "quartier": "$borough", "adresse": "$address.street"},
        # $addToSet crée un ensemble des grades uniques pour ce restaurant
        "LesGrades": {"$addToSet": "$grades.grade"}
    }},
    {"$match": {
        # $and : toutes les conditions doivent être vraies
        "$and": [
            # Vérifie que le tableau n'a qu'un seul élément unique
            {"LesGrades": {"$size": 1}},
            # Vérifie que cet élément est "A"
            {"LesGrades": {"$eq": ["A"]}}
        ]
    }},
    {"$project": {
        # Extrait les champs du sous-document _id
        "nom": "$_id.nom",
        "Quartier": "$_id.quartier",
        "Adresse": "$_id.addresse",
        "_id": 0
    }}
])))

# 9. Compter le nombre d'évaluation par jour de la semaine  
print(pandas.DataFrame(db.restaurants.aggregate([
    {"$unwind": "$grades"},
    {"$project": {
        # $dayOfWeek extrait le jour de la semaine (1=dimanche, 7=samedi)
        "dayOfWeek": {"$dayOfWeek": "$grades.date"}
    }},
    # Regroupe par jour de la semaine et compte les évaluations
    {"$group": {"_id": "$dayOfWeek", "nb evaluations": {"$sum": 1}}},
    {"$sort": {"nb evaluations": -1}},
])))

# 10. Donner les 3 types de cuisine les plus présents par quartier
df = pandas.DataFrame(db.restaurants.aggregate([
    # Regroupe par combinaison quartier + cuisine
    {"$group": {"_id": {"quartier": "$borough", "cuisine": "$cuisine"}, "count": {"$sum": 1}}},
    # Trie par quartier puis par nombre (pour avoir les plus fréquents en premier)
    {"$sort": {"_id.quartier": 1, "count": -1}},
    {"$group": {
        # Regroupe par quartier
        "_id": "$_id.quartier",
        # $push ajoute chaque cuisine et son nombre dans un tableau
        "top_cuisines": {"$push": {"cuisine": "$_id.cuisine", "nb": "$count"}}
    }},
    {"$project": {
        # $slice extrait les 3 premiers éléments du tableau
        "top_cuisines": {"$slice": ["$top_cuisines", 3]}
    }}
]))
