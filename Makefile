# Variables de configuration globale
PROJECT_NAME=nexus-energy-data
ENV_FILE=.env
PYTHON_VENV=.venv
PYTHON_SCRIPT=simulation/simulateur.py


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
  pipeline.batch.size: 3000
  queue.type: persisted

- pipeline.id: energy-sftp
  path.config: "/usr/share/logstash/pipeline/02-input-sftp.conf"
  pipeline.workers: 2
  pipeline.batch.size: 500
  queue.type: memory

- pipeline.id: central-elastic-sink
  path.config: "/usr/share/logstash/pipeline/99-output-elastic.conf"
  pipeline.workers: 4
  pipeline.batch.size: 3500
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
  # On extrait le vrai timestamp du simulateur pour écraser le @timestamp de Logstash
  if [timestamp] {
    date {
      match => [ "timestamp", "ISO8601" ]
      target => "@timestamp"
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

  # Clé déterministe = même paire (dimension, timestamp) que la TSDS utilise
  # nativement pour dédupliquer. Sert uniquement à la branche index classique.
  # IMPORTANT : LogStash::Timestamp.getTime() retourne l'epoch en millisecondes
  # (méthode Java Joda-Time, fiable en JRuby). C'est exactement la granularité
  # que le TSDS utilise pour sa clé de déduplication (dimension, @timestamp en ms).
  ruby {
    code => "
      ts = event.get('@timestamp')
      if ts
        event.set('[@metadata][ts_ms]', ts.getTime().to_s)
      end
    "
  }
  fingerprint {
    source => ["turbine_id", "[@metadata][ts_ms]"]
    target => "[@metadata][doc_id]"
    method => "SHA256"
    concatenate_sources => true
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
      # type "logs" : événement discret, pas de série temporelle continue
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
    key    => "$${FINGERPRINT_KEY}"
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
  pipeline {
    address => "central-elastic-sink"
  }
}

output {
  # ============================================================
  # BRANCHE 1 : data stream (TSDS pour iot, classique pour maintenance)
  # ============================================================
  if [data_stream][type] {
    elasticsearch {
      hosts => ["$${ELASTIC_INETRNE_URL}"]
      user => "$${ELASTIC_USER}"
      password => "$${ELASTIC_PASSWORD}"
      ssl_enabled => true
      ssl_certificate_authorities => ["/usr/share/logstash/config/certs/ca/ca.crt"]

      action => "create"
      data_stream => "true"
      # Valeur de repli obligatoire (littéral requis par le plugin,
      # pas d'interpolation dynamique possible ici). Sans effet réel :
      # le champ [data_stream][type] déjà présent sur l'événement
      # (positionné par les filtres d'input) prend toujours le dessus
      # via l'auto-routing du plugin.
      data_stream_type => "logs"
      data_stream_dataset => "%{[data_stream][dataset]}"
      data_stream_namespace => "%{[data_stream][namespace]}"
      manage_template => false
    }
  }

  # ============================================================
  # BRANCHE 2 : index classique — écriture DUPLIQUÉE, pour le benchmark
  # ============================================================
  if [data_stream][dataset] == "energy.iot" {
    elasticsearch {
      hosts => ["$${ELASTIC_INETRNE_URL}"]
      user => "$${ELASTIC_USER}"
      password => "$${ELASTIC_PASSWORD}"
      ssl_enabled => true
      ssl_certificate_authorities => ["/usr/share/logstash/config/certs/ca/ca.crt"]

      action => "create"
      document_id => "%{[@metadata][doc_id]}"
      data_stream => "false"
      index => "metrics-energy-iot-classic"
      manage_template => false
    }
  } else if [data_stream][dataset] == "energy.maintenance" {
    elasticsearch {
      hosts => ["$${ELASTIC_INETRNE_URL}"]
      user => "$${ELASTIC_USER}"
      password => "$${ELASTIC_PASSWORD}"
      ssl_enabled => true
      ssl_certificate_authorities => ["/usr/share/logstash/config/certs/ca/ca.crt"]

      action => "index"
      data_stream => "false"
      index => "logs-energy-maintenance-classic"
      manage_template => false
    }
  }
}
endef
export CONFIG_OUTPUT


.PHONY: help setup start stop status clean test-load init-es-template init-kibana-dataviews init-kibana-user pipelines
help:
	@echo "Commandes disponibles pour le projet $(PROJECT_NAME) :"
	@echo "  make pipelines             - Crée automatiquement l'arborescence et écrit tous les fichiers ETL"
	@echo "  make setup                 - Initialise l'environnement virtuel Python et les répertoires"
	@echo "  make up                    - Lance l'infrastructure Docker (ES, Kafka, Logstash)"
	@echo "  make init-es-template      - Injecte la politique ILM 24h et les templates d'index TSDS/classiques Elastic 8.x"
	@echo "  make init-downsampling     - Configure le downsampling sur le data stream TSDS energy-iot"
	@echo "                               (à lancer APRÈS un premier 'make test-load', une fois le data stream créé)"
	@echo "  make init-kibana-dataviews - Crée les data views Kibana pour les deux data streams énergie"
	@echo "  make init-kibana-user      - Initialise le mot de passe de l'utilisateur système Kibana"
	@echo "  make test-load             - Démarre le script de génération/simulation multi-sources"
	@echo "  make status                - Vérifie la santé de l'écosystème"
	@echo "  make stop                  - Arrête l'infrastructure"
	@echo "  make clean                 - Supprime les conteneurs et purge les caches locaux"
	@echo "  make reset-benchmark       - Réinitialise les index pour un benchmark propre (supprime tout)"
	@echo "  make benchmark-stats       - Génère le tableau comparatif TSDS vs Classique via le script Python"
	@echo "  make force-downsample      - Force l'exécution immédiate du downsampling sur le backing index TSDS"



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

up:
	@echo "[DOCKER] Lancement des services en arrière-plan..."
	docker compose up -d
	@echo "[DOCKER] En attente de l'activation d'Elasticsearch..."
	docker compose ps --format "{{.Name}}\t{{.State}}" 

init-es-template:
	@echo "[ELASTIC] Création de la politique ILM (Rollover quotidien 24h)..."
	@curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -XPUT "$(ELASTIC_URL)/_ilm/policy/metrics-lifecycle-policy" \
		-H 'Content-Type: application/json' \
		-d '{"policy":{"phases":{"hot":{"actions":{"rollover":{"max_age":"1d"}}}}}}'
	@echo "\n[ELASTIC] Injection template TSDS energy-iot..."
	@curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -XPUT "$(ELASTIC_URL)/_index_template/metrics-energy.iot-template" \
		-H 'Content-Type: application/json' \
		-d '{"index_patterns":["metrics-energy.iot*"],"data_stream":{},"priority":600,"template":{"settings":{"index.mode":"time_series","index.lifecycle.name":"metrics-lifecycle-policy"},"mappings":{"properties":{"@timestamp":{"type":"date"},"turbine_id":{"type":"keyword","time_series_dimension":true},"status":{"type":"keyword"},"wind_speed":{"type":"half_float","time_series_metric":"gauge"},"power_output":{"type":"float","time_series_metric":"gauge"}}}}}'
	@echo "\n[ELASTIC] Injection template classique energy-maintenance..."
	@curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -XPUT "$(ELASTIC_URL)/_index_template/logs-energy-maintenance-template" \
		-H 'Content-Type: application/json' \
		-d '{"index_patterns":["logs-energy.maintenance*"],"data_stream":{},"priority":600,"template":{"settings":{"index.lifecycle.name":"metrics-lifecycle-policy"},"mappings":{"properties":{"@timestamp":{"type":"date"},"log_data":{"properties":{"asset_id":{"type":"keyword"},"technician_id_hashed":{"type":"keyword"},"action_performed":{"type":"keyword"},"vibration_level":{"type":"float"}}}}}}}'
	@echo "\n[ELASTIC] Augmentation search.max_buckets..."
	@curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -XPUT "$(ELASTIC_URL)/_cluster/settings" \
		-H 'Content-Type: application/json' \
		-d '{"persistent":{"search.max_buckets":100000}}'
	@echo "\n[ELASTIC] Injection template classique energy-iot..."
	@curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -XPUT "$(ELASTIC_URL)/_index_template/energy-iot-classic-template" \
		-H 'Content-Type: application/json' \
		-d '{"index_patterns":["metrics-energy-iot-classic*"],"priority":500,"template":{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"@timestamp":{"type":"date"},"turbine_id":{"type":"keyword"},"status":{"type":"keyword"},"wind_speed":{"type":"half_float"},"power_output":{"type":"float"}}}}}'
	@echo "\n[ELASTIC] Injection template classique energy-maintenance..."
	@curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -XPUT "$(ELASTIC_URL)/_index_template/energy-maintenance-classic-template" \
		-H 'Content-Type: application/json' \
		-d '{"index_patterns":["metrics-energy-maintenance-classic*","logs-energy-maintenance-classic*"],"priority":500,"template":{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"@timestamp":{"type":"date"},"log_data":{"properties":{"asset_id":{"type":"keyword"},"technician_id_hashed":{"type":"keyword"},"action_performed":{"type":"keyword"},"vibration_level":{"type":"float"}}}}}}}'
	@echo "\n[ELASTIC] Politique ILM, templates et paramètres du cluster configurés avec succès."

init-downsampling:
	@echo "[ELASTIC] Vérification de l'existence du data stream metrics-energy.iot-default..."
	@curl -sk -o /dev/null -w "%{http_code}" -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" \
		"$(ELASTIC_URL)/_data_stream/metrics-energy.iot-default" | grep -q "^200" \
		&& echo "[ELASTIC] Data stream trouvé, configuration du downsampling..." \
		|| (echo "[ELASTIC] ERREUR : le data stream n'existe pas encore. Lancez d'abord 'make test-load' puis réessayez." && exit 1)
	@curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -XPUT "$(ELASTIC_URL)/_data_stream/metrics-energy.iot-default/_lifecycle" \
		-H 'Content-Type: application/json' \
		-d '{"data_retention":"90d","downsampling":[{"after":"1d","fixed_interval":"10m"},{"after":"30d","fixed_interval":"1h"}]}'
	@echo "\n[ELASTIC] Downsampling configuré : 10m après 1j, 1h après 30j (rétention totale 90j)."

init-kibana-user: ## Initialise le mot de passe de l'utilisateur système kibana_system
	@echo "🔑 Configuration du mot de passe pour kibana_system..."
	@curl -u $(ELASTIC_USER):$(ELASTIC_PASSWORD) -k -X POST "$(ELASTIC_URL)/_security/user/kibana_system/_password" \
		-H "Content-Type: application/json" \
		-d '{"password":"$(KIBANA_PASSWORD)"}'
	@echo "\n✅ Mot de passe mis à jour avec succès dans Elasticsearch."
	@echo "⚠️  N'oublie pas de mettre à jour ELASTICSEARCH_PASSWORD=$(KIBANA_PASSWORD) dans ton docker-compose !"

init-kibana-dataviews:
	@echo "[KIBANA] Création des data views Kibana pour les data streams énergie..."
	docker exec nexus-kibana bash -lc 'curl -sS -k -X POST "$(KIBANA_URL)/api/data_views/data_view" -H "kbn-xsrf: true" -H "Content-Type: application/json" -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -d '\''{"data_view":{"title":"metrics-energy.iot-default*","timeFieldName":"@timestamp","allowNoIndex":true}}'\'' || true'
	@echo ""
	docker exec nexus-kibana bash -lc 'curl -sS -k -X POST "$(KIBANA_URL)/api/data_views/data_view" -H "kbn-xsrf: true" -H "Content-Type: application/json" -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -d '\''{"data_view":{"title":"logs-energy.maintenance-default*","timeFieldName":"@timestamp","allowNoIndex":true}}'\'' || true'
	@echo "[KIBANA] Data views créés (ou déjà existants)."

test-load:
	@echo "[PYTHON] Vérification des dépendances Python dans l'environnement virtuel..."
	@$(PYTHON_VENV)/bin/python3 -c "import dotenv" 2>/dev/null || ( \
		echo "[PYTHON] Module 'python-dotenv' manquant. Installation automatique..." && \
		$(PYTHON_VENV)/bin/pip install python-dotenv \
	)
	@echo "[PYTHON] Activation du simulateur de données Haute performance (20Go-50Go stress-test)..."
	@$(PYTHON_VENV)/bin/python3 $(PYTHON_SCRIPT)

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

reset-benchmark:
	@echo "[RESET] Suppression des index et data streams pour benchmark propre..."
	@-curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -X DELETE "$(ELASTIC_URL)/metrics-energy-iot-classic"
	@-curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -X DELETE "$(ELASTIC_URL)/logs-energy-maintenance-classic"
	@-curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -X DELETE "$(ELASTIC_URL)/_data_stream/logs-energy.maintenance-default"
	@-curl -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -X DELETE "$(ELASTIC_URL)/_data_stream/metrics-energy.iot-default"
	@echo "[RESET] Index supprimés. Redémarrage de Logstash..."
	#@docker restart nexus-logstash
	@echo "[RESET] Attente du démarrage de Logstash..."
	#@sleep 10
	@echo "[RESET] Benchmark prêt. Lancez 'make test-load' pour démarrer la simulation."

benchmark-stats: ## Génère le tableau comparatif TSDS vs Classique via le script Python
	@echo "[BENCHMARK] Récupération des métriques d'indexation depuis Elasticsearch..."
	@curl -s -k -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" "$(ELASTIC_URL)/_stats?expand_wildcards=all" | python3 generate_bilan.py

  .PHONY: force-downsample

force-downsample: ## Force le Downsampling immédiat sur le backing index TSDS
	@echo "[ELASTIC] Détection du backing index TSDS..."
	@BACKING_INDEX=$$(curl -sk -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" "$(ELASTIC_URL)/_cat/indices/.ds-metrics-energy.iot-default*?h=index" | head -n 1 | tr -d '\r') ; \
	if [ -z "$$BACKING_INDEX" ]; then \
		echo "❌ Aucun backing index TSDS trouvé pour metrics-energy.iot-default." ; exit 1 ; \
	fi ; \
	echo "[ELASTIC] Backing index identifié : $$BACKING_INDEX" ; \
	echo "[ELASTIC] Vérification / Application du blocage en écriture (Read-Only)..." ; \
	curl -sk -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -X PUT "$(ELASTIC_URL)/$$BACKING_INDEX/_settings" \
		-H "Content-Type: application/json" -d '{"index.blocks.write": true}' ; \
	echo "\n[ELASTIC] Lancement de l'agrégation Downsampling (intervalle 1h)..." ; \
	curl -sk -u "$(ELASTIC_USER):$(ELASTIC_PASSWORD)" -X POST "$(ELASTIC_URL)/$$BACKING_INDEX/_downsample/downsampled-metrics-energy-iot-1h" \
		-H "Content-Type: application/json" -d '{"fixed_interval": "1h"}' ; \
	echo "\n✅ Downsampling généré avec succès dans 'downsampled-metrics-energy-iot-1h'."