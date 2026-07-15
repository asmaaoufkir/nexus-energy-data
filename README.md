
# Project: nexus-energy-data
> **Architecte Data Senior (14+ ans d'expérience)** > Scope: Ingestion industrielle multi-sources multi-pipelines (Volume cible: 2 To/an)  
> Stack: Logstash 8.11.3 (Isolate Processing Mode), Elasticsearch 8.11.3 TSDS Active.

## Architecture & Routage Isolé
Le projet orchestre le traitement de flux asynchrones cloisonnés au sein du fichier `pipelines.yml` afin de prévenir tout phénomène de contre-pression (*backpressure*). 
- **Pipeline 1 (energy-iot)** : Consomme le streaming temps réel Kafka (IoT Éolien). File d'attente persistante disque activée (`queue.type: persisted`).
- **Pipeline 2 (energy-sftp)** : Absorbe les batchs horaires de rapports de maintenance via fichiers CSV partagés.
- **Pipeline 3 (elastic-output)** : Sink centralisé d'écriture via le bus mémoire interne (`pipeline` communication), isolant l'accès sécurisé à Elasticsearch.
- **Service 4 (Kibana)** : Visualisation sécurisée des data streams Elastic `metrics-energy.iot-default` et `logs-energy.maintenance-default`.

## Kibana et dashboards énergie
Ce projet expose Kibana en HTTPS sur `https://localhost:5601`.

Points de sécurité et bonnes pratiques :
- TLS activé côté Elasticsearch et Kibana avec certificats auto-signés validés par la même CA.
- Authentification Elasticsearch via l’utilisateur `elastic` et mot de passe secret stocké dans `.env`.
- `xpack.encryptedSavedObjects.encryptionKey` défini pour protéger les objets sauvegardés Kibana.
- Accès réseau strictement sur `nexus-network` entre les services Docker.

Index patterns à créer dans Kibana :
- `metrics-energy.iot-default*` pour le data stream IoT.
- `logs-energy.maintenance-default*` pour le data stream maintenance.

Dashboards recommandés :
- **Wind Farm Performance Overview** : courbes de tendance sur `avg(power_output)` et `avg(wind_speed)`.
- **Turbine Status & Availability** : camembert / barres de la distribution des statuts `status` et du nombre de turbines actives.
- **Maintenance & Vibration Trends** : visualisations des actions et de la vibration par `asset` et `tech`.
- **Energy Production vs Maintenance Impact** : corrélation entre production et événements métiers de maintenance.

## Déploiement Rapide (Quick Start)

### 1. Pré-requis système
S'assurer que le paramètre kernel système `vm.max_map_count` est correctement configuré pour Elasticsearch :
```bash
sudo sysctl -w vm.max_map_count=262144