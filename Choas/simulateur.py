import json
import time
import random
import csv
import os
from datetime import datetime
from multiprocessing import Process
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration des chemins cibles
CSV_OUTPUT_DIR = "/var/log/energy/sftp"
os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)

# Le script Python tourne sur la machine hôte (via le venv .venv, pas dans un conteneur Donc il faut utiliser le listener EXTERNAL://localhost:9092, pas nexus-kafka:29092 qui est le listener INTERNAL réservé aux conteneurs du réseau nexus-network (Logstash s'y connecte via ce nom, lui, car il est dans le même réseau docker).
def simulate_kafka_iot_stream():
    """Simule l'envoi de métriques IoT Haute fréquence (ex: Éoliennes)"""
    print("[Python Server] Lancement de la simulation IoT Éolien (Kafka réel)...")
    
    # Correction 1 : On crée le producer ICI, dans le process enfant
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Optionnel mais sécurisant : on force l'envoi rapide des petits volumes
        linger_ms=100,
        batch_size=65536 
    )
    
    turbines = [f"TURBINE-NORTH-{i:03d}" for i in range(1, 21)]

    while True:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "turbine_id": random.choice(turbines),
            "wind_speed": round(random.uniform(3.5, 25.0), 2),
            "power_output": round(random.uniform(100.0, 2500.0), 2),
            "status": random.choice(["OPERATIONAL", "OPERATIONAL", "MAINTENANCE_REQUIRED", "STALLED"])
        }
        
        # Envoi du message
        future = producer.send("energy-metrics-iot", value=payload)
        
        # Correction 2 : On force l'envoi physique immédiat pour le test
        #producer.flush() 
        
        print(f"[Python Server] Envoyé sur Kafka: {payload['turbine_id']}")
        #time.sleep(0.5)

def simulate_sftp_csv_batch():
    """Simule la génération horaire de fichiers CSV de maintenance sur un serveur de stockage"""
    print("[Python Server] Lancement du générateur de rapports SFTP (CSV)...")
    assets = [f"PIPELINE-VALVE-{i:02d}" for i in range(1, 10)]
    techs = ["TECH-ALEX-88", "TECH-MARIE-42", "TECH-JEAN-07"]
    actions = ["PRESSURE_CHECK", "VALVE_LUBRICATION", "SENSOR_REPLACEMENT", "ROUTINE_INSPECTION"]
    
    while True:
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(CSV_OUTPUT_DIR, f"maintenance_{timestamp_str}.csv")
        
        # Génération d'un batch de lignes simulant un export
        with open(file_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            for _ in range(random.randint(10, 50)):
                log_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                writer.writerow([
                    log_date,
                    random.choice(techs),
                    random.choice(assets),
                    random.choice(actions),
                    round(random.uniform(0.01, 4.99), 4) # Niveau de vibration
                ])
                
        print(f"[Python Server] Nouveau fichier SFTP batch généré : {file_path}")
        #time.sleep(10) # Simule un intervalle régulier (réduit ici à 10s pour la démonstration)

if __name__ == "__main__":
    # Isolation des simulations dans des processus distincts pour simuler la production
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