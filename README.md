# Projet Outils Big Data : BRISBANE City Bike

En effet, le jeu de données que nous avons utilisé pour réaliser ce travail provient de données des stations vélos dans la ville BRISBANE.
La ville de Brisbane est composée de 149 vélos. Il était question pour nous de trouver un moyen adéquat afin de classifier ces vélos dans des groupes (classes) différents.

Ainsi, l'objectif général de ce travail est de réaliser un Clustering à l’aide de méthode de k-Means et aussi cartographier l'emplacement des stations vélos en utilisant Spark. Le jeu de données contient des informations concernant l’emplacement de chaque vélo est disponible dans le fichier nommé Base.  

## Présentation des Variables
Notre jeu de données est constitué des variables suivantes :

- Adresse : lieu où se trouve les vélos ;
- Latitude : Position latitude ;
- Longitude : Position longitude ;
- name : le numéro plus l'adresse ;
- number : le numéro ou l’identifiant  attribué à chaque vélo

Afin de trouver les k-means nous décidons de garder uniquement les variables longitude ainsi que latitude. D’où le mapping ci-dessous.
