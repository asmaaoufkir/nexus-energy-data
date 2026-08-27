import json
import time
import random
import csv
import os
from datetime import datetime, timezone
from multiprocessing import Process
from kafka import KafkaProducer

CSV_OUTPUT_DIR = "/var/log/energy/sftp"
os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)

def simulate_kafka_iot_stream():
    """Simule l'envoi de métriques IoT Haute fréquence (Optimisé Batching)"""
    print("[Python Server] Lancement de la simulation IoT Éolien (Mode Haute Performance)...")
    
    # ⚡ OPTIMISATION PRODUCER : Batching en mémoire & compression Snappy/Gzip (optionnel)
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20,         # Attend jusqu'à 20ms pour grouper les messages en paquets
        batch_size=64 * 1024, # Paquets de 64 KB (au lieu de 16 KB par défaut)
        acks=1               # Acquittement rapide du broker leader uniquement
    )
    
    turbines = [f"TURBINE-NORTH-{i:03d}" for i in range(1, 100)]
    statuses = ["OPERATIONAL", "OPERATIONAL", "OPERATIONAL", "MAINTENANCE_REQUIRED", "STALLED"]

    total_sent = 0
    start_time = time.time()

    try:
        while True:
            # ⚡ OPTIMISATION : Envoi en rafales (Batches) sans bloquer sur flush()
            for _ in range(5000):  # Envoie 5000 messages par boucle
                payload = {
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "turbine_id": random.choice(turbines),
                    "wind_speed": round(random.uniform(3.5, 25.0), 2),
                    "power_output": round(random.uniform(100.0, 2500.0), 2),
                    "status": random.choice(statuses)
                }
                # Envoi asynchrone dans le buffer mémoire du Producer
                producer.send("energy-metrics-iot", value=payload)
                total_sent += 1

            # Log de débit toutes les n boucles au lieu d'un print par message
            elapsed = time.time() - start_time
            print(f"[Python IoT] {total_sent} messages injectés | Débit moyen: {int(total_sent / elapsed)} msg/sec")
            
            # Pause minime pour ne pas complètement saturer le CPU hôte si besoin (ajustable)
            time.sleep(0.01)

    except KeyboardInterrupt:
        producer.flush()
        producer.close()

def simulate_sftp_csv_batch():
    """Simule la génération rapide de volumineux fichiers CSV de maintenance"""
    print("[Python Server] Lancement du générateur de rapports SFTP (Gros Batches)...")
    assets = [f"PIPELINE-VALVE-{i:02d}" for i in range(1, 50)]
    techs = ["TECH-ALEX-88", "TECH-MARIE-42", "TECH-JEAN-07", "TECH-SARA-19"]
    actions = ["PRESSURE_CHECK", "VALVE_LUBRICATION", "SENSOR_REPLACEMENT", "ROUTINE_INSPECTION"]
    
    while True:
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        file_path = os.path.join(CSV_OUTPUT_DIR, f"maintenance_{timestamp_str}.csv")
        
        # ⚡ OPTIMISATION CSV : Écriture de 10 000 à 50 000 lignes par fichier
        lignes_count = random.randint(10000, 50000)
        
        with open(file_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Pré-génération en mémoire pour écriture I/O disque ultra-rapide
            rows = [
                [
                    now_str,
                    random.choice(techs),
                    random.choice(assets),
                    random.choice(actions),
                    round(random.uniform(0.01, 4.99), 4)
                ]
                for _ in range(lignes_count)
            ]
            writer.writerows(rows)
                
        print(f"[Python SFTP] Généré : {file_path} ({lignes_count} lignes)")
        time.sleep(2) # Génère un gros fichier CSV toutes les 2 secondes

if __name__ == "__main__":
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