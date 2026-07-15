

# Variables de configuration globale
PROJECT_NAME=nexus-energy-data
ENV_FILE=.env
PYTHON_VENV=.venv
PYTHON_SCRIPT=simulation/simulateur.py
ELASTIC_URL=https://localhost:9200
KIBANA_URL=https://localhost:5601
KIBANA_USER=elastic

# Indique à Make de charger le fichier .env s'il existe
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Définition des contenus multi-lignes propres (Syntaxe Senior Make)
define CONFIG_PIPELINES
- pipeline.id: energy-iot
  path.config: "/usr/share/logstash/pipeline/01-input-iot.conf"
  pipeline.workers: 4
  pipeline.batch.size: 500
  queue.type: persisted

- pipeline.id: energy-sftp
  path.config: "/usr/share/logstash/pipeline/02-input-sftp.conf"
  pipeline.workers: 2
  pipeline.batch.size: 125
  queue.type: memory

- pipeline.id: central-elastic-sink
  path.config: "/usr/share/logstash/pipeline/99-output-elastic.conf"
  pipeline.workers: 4
  pipeline.batch.size: 1000
endef
export CONFIG_PIPELINES

define CONFIG_IOT
input {
  kafka {
    bootstrap_servers => "nexus-kafka:29092"
    topics            => ["energy-metrics-iot"]
    group_id          => "logstash-iot-v3"
    auto_offset_reset => "earliest"
    codec             => json {
      target => "[@metadata][raw_json]"
    }
  }
}

filter {
  if [@metadata][raw_json] {
    ruby {
      code => "
        raw = event.get('[@metadata][raw_json]')
        if raw.is_a?(Hash)
          raw.each { |k, v| event.set(k, v) }
        end
      "
    }
  }

  mutate {
    add_field => {
      "[data_stream][type]"      => "metrics"
      "[data_stream][dataset]"   => "energy.iot"
      "[data_stream][namespace]" => "default"
    }
  }

  mutate {
    convert => {
      "wind_speed"   => "float"
      "power_output" => "float"
      "turbine_id"   => "string"
      "status"       => "string"
    }
  }
}

output {
  pipeline {
    send_to => ["central-elastic-sink"]
  }
}
endef
export CONFIG_IOT

define CONFIG_SFTP
input {
  file {
    path => "/var/log/energy/sftp/maintenance_*.csv"
    start_position => "beginning"
    sincedb_path => "/usr/share/logstash/data/sftp_sincedb"
    mode => "tail"
  }
}

filter {
  csv {
    separator => ";"
    columns => ["log_date", "technician_id", "asset_id", "action_performed", "vibration_level"]
    target   => "[log_data]"
  }
  mutate {
	  add_field => {
      "[data_stream][type]"      => "logs"
      "[data_stream][dataset]"   => "energy.maintenance"
      "[data_stream][namespace]" => "default"
    }
    convert => { "[log_data][vibration_level]" => "float" }
  }
  fingerprint {
    source => ["[log_data][technician_id]"]
    target => "[log_data][technician_id_hashed]"
    method => "SHA256"
    key    => "ShellSeniorSecretKey2026"
    remove_field => ["[log_data][technician_id]"]
  }
  date {
    match  => [ "[log_data][log_date]", "yyyy-MM-dd HH:mm:ss" ]
    timezone => "Europe/Paris"
    target => "@timestamp"
    remove_field => [ "[log_data][log_date]" ]
  }
}

output {
  pipeline { send_to => "central-elastic-sink" }
}
endef
export CONFIG_SFTP

define CONFIG_OUTPUT
input {
  pipeline { address => "central-elastic-sink" }
}

output {
  elasticsearch {
    hosts => ["https://nexus-elasticsearch:9200"]
    user => "$${ES_INGEST_USER}"
    password => "$${ELASTIC_PASSWORD}"
	  ssl_enabled => true
    ssl_certificate_authorities => ["/usr/share/logstash/config/certs/ca/ca.crt"]
    action => "create"
	  data_stream => "true"

  }
}
endef
export CONFIG_OUTPUT


.PHONY: help setup start stop status clean test-load init-es-template init-kibana-dataviews init-kibana-user pipelines
help:
	@echo "Commandes disponibles pour le projet $(PROJECT_NAME) :"
	@echo "  make pipelines         - Crée automatiquement l'arborescence et écrit tous les fichiers ETL"
	@echo "  make setup             - Initialise l'environnement virtuel Python et les répertoires"
	@echo "  make start             - Lance l'infrastructure Docker (ES, Kafka, Logstash)"
	@echo "  make init-es-template  - Injecte le template d'index TSDS Elastic 8.x officiel"
	@echo "  make init-kibana-dataviews - Crée les data views Kibana pour les deux data streams énergie"
	@echo "  make init-kibana-user - Initialise le mot de passe de l'utilisateur système Kibana"
	@echo "  make test-load         - Démarre le script de génération/simulation multi-sources"
	@echo "  make status            - Vérifie la santé de l'écosystème"
	@echo "  make stop              - Arrête l'infrastructure"
	@echo "  make clean             - Supprime les conteneurs et purge les caches locaux"


pipelines:
	@echo "[pipelines] Création de l'arborescence des dossiers..."
	mkdir -p pipeline/conf.d
	mkdir -p simulation
	
	@echo "[pipelines] Écriture de pipeline/pipelines.yml..."
	@echo "$$CONFIG_PIPELINES" > pipeline/pipelines.yml
	
	@echo "[pipelines] Écriture de pipeline/conf.d/01-input-iot.conf..."
	@echo "$$CONFIG_IOT" > pipeline/conf.d/01-input-iot.conf
	
	@echo "[pipelines] Écriture de pipeline/conf.d/02-input-sftp.conf..."
	@echo "$$CONFIG_SFTP" > pipeline/conf.d/02-input-sftp.conf
	
	@echo "[pipelines] Écriture de pipeline/conf.d/99-output-elastic.conf..."
	@echo "$$CONFIG_OUTPUT" > pipeline/conf.d/99-output-elastic.conf
	
	@echo "[pipelines] Architecture de pipelines initialisée avec une indentation parfaite !"

setup:
	@echo "[INIT] Création des dossiers locaux de stockage..."
	sudo mkdir -p /var/log/energy/sftp
	sudo chmod -R 777 /var/log/energy/sftp
	@echo "[INIT] Initialisation de l'environnement virtuel Python..."
	python3 -m venv $(PYTHON_VENV)
	@echo "[INIT] Installation des dépendances Python..."
	$(PYTHON_VENV)/bin/pip install kafka-python
	@echo "[INIT] Environnement prêt."

start:
	@echo "[DOCKER] Lancement des services en arrière-plan..."
	docker compose up -d
	@echo "[DOCKER] En attente de l'activation d'Elasticsearch..."
	docker compose ps

init-es-template:
	@echo "[ELASTIC] Injection du template d'index TSDS officiel..."
	curl -k -u "$(ES_INGEST_USER):$(ELASTIC_PASSWORD)" -XPUT "$(ELASTIC_URL)/_index_template/energy-tsds-template" \
		-H 'Content-Type: application/json' \
		-d '{"index_patterns":["metrics-energy.*"],"data_stream":{},"priority":500,"template":{"settings":{"index.mode":"time_series","index.lifecycle.name":"metrics-lifecycle-policy"},"mappings":{"properties":{"@timestamp":{"type":"date"},"turbine_id":{"type":"keyword","time_series_dimension":true},"status":{"type":"keyword"},"wind_speed":{"type":"half_float","time_series_metric":"gauge"},"power_output":{"type":"float","time_series_metric":"gauge"}}}}}'
	@echo "\n[ELASTIC] Augmentation de la limite maximale de buckets (search.max_buckets)..."
	curl -k -u "$(ES_INGEST_USER):$(ELASTIC_PASSWORD)" -XPUT "$(ELASTIC_URL)/_cluster/settings" \
		-H 'Content-Type: application/json' \
		-d '{"persistent":{"search.max_buckets":100000}}'
	@echo "\n[ELASTIC] Template injecté et paramètres du cluster configurés avec succès."


init-kibana-user: ## Initialise le mot de passe de l'utilisateur système kibana_system
	@echo "🔑 Configuration du mot de passe pour kibana_system..."
	@curl -u $(ES_INGEST_USER):$(ELASTIC_PASSWORD) -k -X POST "$(ELASTIC_URL)/_security/user/kibana_system/_password" \
		-H "Content-Type: application/json" \
		-d '{"password":"$(KIBANA_PASSWORD)"}'
	@echo "\n✅ Mot de passe mis à jour avec succès dans Elasticsearch."
	@echo "⚠️  N'oublie pas de mettre à jour ELASTICSEARCH_PASSWORD=$(KIBANA_PASSWORD) dans ton docker-compose !"

init-kibana-dataviews:
	@echo "[KIBANA] Création des data views Kibana pour les data streams énergie..."
	docker exec nexus-kibana bash -lc 'curl -sS -k -X POST "$(KIBANA_URL)/api/data_views/data_view" -H "kbn-xsrf: true" -H "Content-Type: application/json" -u "$(ES_INGEST_USER):$(ELASTIC_PASSWORD)" -d '\''{"data_view":{"title":"metrics-energy.iot-default*","timeFieldName":"@timestamp","allowNoIndex":true}}'\'' || true'
	@echo ""
	docker exec nexus-kibana bash -lc 'curl -sS -k -X POST "$(KIBANA_URL)/api/data_views/data_view" -H "kbn-xsrf: true" -H "Content-Type: application/json" -u "$(ES_INGEST_USER):$(ELASTIC_PASSWORD)" -d '\''{"data_view":{"title":"logs-energy.maintenance-default*","timeFieldName":"@timestamp","allowNoIndex":true}}'\'' || true'
	@echo "[KIBANA] Data views créés (ou déjà existants)."

test-load:
	@echo "[PYTHON] Activation du simulateur de données Haute performance (20Go-50Go stress-test)..."
	$(PYTHON_VENV)/bin/python3 $(PYTHON_SCRIPT)

status:
	@echo "[STATUS] Conteneurs :"
	docker compose ps
	@echo "[STATUS] Volume SFTP local :"
	ls -lh /var/log/energy/sftp/ | head -n 5

stop:
	@echo "[DOCKER] Arrêt des services..."
	docker compose down

clean: stop
	@echo "[CLEAN] Purge complète des volumes persistants et résidus..."
	docker compose down -v
	rm -rf $(PYTHON_VENV)