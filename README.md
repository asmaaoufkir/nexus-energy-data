# Nexus Energy Data

> Projet de plateforme de données industrielle orientée énergie, conçu pour l’ingestion multi-sources, le traitement temps réel et la visualisation métier.

## Vue d’ensemble

Nexus Energy Data est une architecture Data Engineering complète mettant en œuvre :

- ingestion de données IoT depuis Kafka
- ingestion de logs de maintenance depuis des fichiers CSV via SFTP
- traitement et routage via Logstash
- stockage et analyse dans Elasticsearch 8.x
- visualisation métier dans Kibana

Le projet est pensé pour illustrer une approche robuste, sécurisée et prête pour une présentation technique à un recruteur ou à un comité technique.

## Architecture

Le projet repose sur les composants suivants :

- Python Simulator : génération de données de test depuis la machine hôte
- Kafka : consommation du flux temps réel IoT
- Logstash : traitement des pipelines d’ingestion et d’export
- Elasticsearch : stockage dans des data streams TSDS
- Kibana : tableaux de bord et exploration métier
- Docker Compose : orchestration de la stack
- Makefile : automatisation des opérations clés du projet

## Fonctionnement métier

Le flux de données couvre deux cas d’usage principaux :

1. Données IoT énergétiques
   - métriques de performance des turbines
   - vitesse du vent
   - puissance produite
   - statut opérationnel

2. Données de maintenance industrielle
   - fichiers CSV de rapports de maintenance
   - interventions techniques
   - niveaux de vibration
   - actifs critiques et techniciens

## Structure du projet

```text
.
├── docker-compose.yml
├── Makefile
├── README.md
├── pipeline/
│   ├── pipelines.yml
│   └── conf.d/
├── simulation/
│   └── simulateur.py
└── Docs/
```

## Prérequis

Avant de démarrer, assurez-vous d’avoir :

- Docker et Docker Compose installés
- Python 3.x
- accès aux ports locaux suivants : 9200, 5601, 9092, 5044
- un fichier `.env` correctement renseigné avec les variables suivantes :
  - `ELASTIC_PASSWORD`
  - `KIBANA_PASSWORD`
  - `KIBANA_ENCRYPTION_KEY`
  - `ES_INGEST_USER`

Il est également recommandé de configurer le paramètre système suivant :

```bash
sudo sysctl -w vm.max_map_count=262144
```

## Démarrage rapide

### 1. Initialiser l’environnement

```bash
make setup
```

Cette commande prépare :

- les répertoires de stockage locaux
- l’environnement virtuel Python
- les dépendances nécessaires

### 2. Générer les pipelines Logstash

```bash
make pipelines
```

Cette commande crée les fichiers de configuration suivants :

- `pipeline/pipelines.yml`
- `pipeline/conf.d/01-input-iot.conf`
- `pipeline/conf.d/02-input-sftp.conf`
- `pipeline/conf.d/99-output-elastic.conf`

### 3. Démarrer la stack

```bash
make start
```

Le démarrage lance les services suivants :

- Elasticsearch
- Kafka
- Logstash
- Kibana

### 4. Initialiser Elasticsearch et Kibana

```bash
make init-es-template
make init-kibana-user
make init-kibana-dataviews
```

Ces étapes permettent de :

- injecter le template TSDS Elasticsearch
- configurer les identifiants Kibana
- créer les data views métier

### 5. Lancer la simulation de charge

```bash
make test-load
```

Cette commande démarre le simulateur Python pour générer des données de test et alimenter les pipelines.

## Commandes principales du Makefile

```bash
make help         # Affiche la liste des commandes disponibles
make setup        # Initialise l’environnement Python et les répertoires
make pipelines    # Génère les fichiers de configuration Logstash
make start        # Démarre la stack Docker
make status       # Vérifie l’état des services
make stop         # Arrête les services
make clean        # Supprime les conteneurs et les volumes associés
```

## Accès aux services

- Elasticsearch : `https://localhost:9200`
- Kibana : `https://localhost:5601`
- Kafka : `localhost:9092`
- Logstash : `localhost:5044`

## Pipelines implémentés

### Pipeline IoT

- consommation de messages Kafka
- transformation des données JSON
- enrichissement des métadonnées
- routage vers Elasticsearch

### Pipeline SFTP

- lecture de fichiers CSV dans un répertoire partagé
- parsing des logs de maintenance
- hashage de certains identifiants pour la confidentialité
- transformation et écriture vers Elasticsearch

### Sink Elasticsearch

- écriture centralisée vers les data streams
- isolation du traitement de sortie
- intégration avec la couche de stockage analytique

## Observabilité et supervision

Pour surveiller la santé de l’infrastructure :

```bash
make status
docker stats nexus-elasticsearch nexus-logstash nexus-kafka nexus-kibana
```

## Points forts techniques

Ce projet met en évidence plusieurs compétences clés de niveau senior :

- architecture événementielle multi-sources
- pipelines de traitement isolés et sécurisés
- utilisation de data streams Elasticsearch orientés séries temporelles
- intégration Docker / orchestration locale
- automatisation avec Makefile
- préparation d’un environnement de démonstration professionnelle

## Objectif de présentation

Ce projet peut être présenté comme une preuve concrète de compétence en :

- Data Engineering
- ingestion temps réel et batch
- architecture data moderne
- observabilité et exploitation
- conception de solutions orientées business et opérationnelles
