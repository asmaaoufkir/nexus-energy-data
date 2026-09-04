import csv
import json
import os
import random
import time
from datetime import datetime, timezone
from multiprocessing import Process
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaProducer

# 💡 Chargement dynamique du fichier .env situé à la racine du projet (1 dossier au-dessus)
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# ⚙️ Récupération des variables d'environnement
ELASTIC_URL = os.getenv("ELASTIC_URL", "https://localhost:9200")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CSV_OUTPUT_DIR = os.getenv("CSV_OUTPUT_DIR", "/var/log/energy/sftp")

# Création du dossier cible si inexistant
os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)


def simulate_kafka_iot_stream():
    """Simule l'envoi de métriques IoT Haute fréquence (Optimisé Batching)."""
    print(
        f"[Python Server] Lancement de la simulation IoT Éolien vers Kafka ({KAFKA_BOOTSTRAP_SERVERS})..."
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20,  # Attend jusqu'à 20ms pour grouper les messages en paquets
        batch_size=64 * 1024,  # Paquets de 64 KB
        acks=1,  # Acquittement rapide du broker leader uniquement
    )

    turbines = [f"TURBINE-NORTH-{i:03d}" for i in range(1, 100)]
    statuses = [
        "OPERATIONAL",
        "OPERATIONAL",
        "OPERATIONAL",
        "MAINTENANCE_REQUIRED",
        "STALLED",
    ]

    total_sent = 0
    start_time = time.time()

    try:
        while True:
            # Envoi en rafales (Batches)
            for _ in range(5000):
                payload = {
                    "timestamp": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "turbine_id": random.choice(turbines),
                    "wind_speed": round(random.uniform(3.5, 25.0), 2),
                    "power_output": round(random.uniform(100.0, 2500.0), 2),
                    "status": random.choice(statuses),
                }
                producer.send("energy-metrics-iot", value=payload)
                total_sent += 1

            elapsed = time.time() - start_time
            print(
                f"[Python IoT] {total_sent} messages injectés | Débit moyen: {int(total_sent / elapsed)} msg/sec"
            )

            time.sleep(0.01)

    except KeyboardInterrupt:
        producer.flush()
        producer.close()


def simulate_sftp_csv_batch():
    """Simule la génération rapide de volumineux fichiers CSV de maintenance."""
    print(
        f"[Python Server] Lancement du générateur de rapports SFTP dans {CSV_OUTPUT_DIR}..."
    )
    assets = [f"PIPELINE-VALVE-{i:02d}" for i in range(1, 50)]
    techs = ["TECH-ALEX-88", "TECH-MARIE-42", "TECH-JEAN-07", "TECH-SARA-19"]
    actions = [
        "PRESSURE_CHECK",
        "VALVE_LUBRICATION",
        "SENSOR_REPLACEMENT",
        "ROUTINE_INSPECTION",
    ]

    while True:
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        file_path = os.path.join(
            CSV_OUTPUT_DIR, f"maintenance_{timestamp_str}.csv"
        )

        lignes_count = random.randint(10000, 50000)

        with open(file_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=";")
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            rows = [
                [
                    now_str,
                    random.choice(techs),
                    random.choice(assets),
                    random.choice(actions),
                    round(random.uniform(0.01, 4.99), 4),
                ]
                for _ in range(lignes_count)
            ]
            writer.writerows(rows)

        print(f"[Python SFTP] Généré : {file_path} ({lignes_count} lignes)")
        time.sleep(2)


if __name__ == "__main__":
    print(f"[Config] ELASTIC_URL chargé : {ELASTIC_URL}")
    print(f"[Config] KAFKA_BOOTSTRAP_SERVERS chargé : {KAFKA_BOOTSTRAP_SERVERS}")

    p1 = Process(target=simulate_kafka_iot_stream)
    p2 = Process(target=simulate_sftp_csv_batch)

    try:
        p1.start()
        p2.start()
        p1.join()
        p2.join()
    except KeyboardInterrupt:
        print("\n[Python Server] Arrêt des simulateurs de données.")
        p1.terminate()
        p2.terminate()