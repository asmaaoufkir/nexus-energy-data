#!/bin/bash
# Script de vérification des compteurs de documents pour le benchmark TSDS vs Classic

ELASTIC_URL="https://localhost:9200"
ELASTIC_USER="elastic"
ELASTIC_PASSWORD="ShellSeniorSecretKey2026"

echo "=== Vérification des compteurs de documents ==="
echo ""

# Récupération des compteurs
TSDS_IOT=$(curl -s -k -u "$ELASTIC_USER:$ELASTIC_PASSWORD" -X GET "$ELASTIC_URL/_cat/indices/.ds-metrics-energy.iot-default*?h=docs.count" | awk '{s+=$1} END {print s}')
CLASSIC_IOT=$(curl -s -k -u "$ELASTIC_USER:$ELASTIC_PASSWORD" -X GET "$ELASTIC_URL/_cat/indices/metrics-energy-iot-classic?h=docs.count" | awk '{s+=$1} END {print s}')

TSDS_MAINT=$(curl -s -k -u "$ELASTIC_USER:$ELASTIC_PASSWORD" -X GET "$ELASTIC_URL/_cat/indices/.ds-logs-energy.maintenance-default*?h=docs.count" | awk '{s+=$1} END {print s}')
CLASSIC_MAINT=$(curl -s -k -u "$ELASTIC_USER:$ELASTIC_PASSWORD" -X GET "$ELASTIC_URL/_cat/indices/logs-energy-maintenance-classic?h=docs.count" | awk '{s+=$1} END {print s}')

# Affichage des résultats
echo "IoT Metrics:"
echo "  TSDS (data stream)    : $TSDS_IOT docs"
echo "  Classic (index)       : $CLASSIC_IOT docs"
if [ ! -z "$TSDS_IOT" ] && [ ! -z "$CLASSIC_IOT" ]; then
    DIFF_IOT=$((CLASSIC_IOT - TSDS_IOT))
    PERCENT_IOT=$(echo "scale=2; ($DIFF_IOT * 100) / $TSDS_IOT" | bc)
    echo "  Écart                 : $DIFF_IOT docs ($PERCENT_IOT%)"
fi
echo ""

echo "Maintenance Logs:"
echo "  TSDS (data stream)    : $TSDS_MAINT docs"
echo "  Classic (index)       : $CLASSIC_MAINT docs"
if [ ! -z "$TSDS_MAINT" ] && [ ! -z "$CLASSIC_MAINT" ]; then
    DIFF_MAINT=$((CLASSIC_MAINT - TSDS_MAINT))
    PERCENT_MAINT=$(echo "scale=2; ($DIFF_MAINT * 100) / $TSDS_MAINT" | bc)
    echo "  Écart                 : $DIFF_MAINT docs ($PERCENT_MAINT%)"
fi
echo ""

# Vérification des erreurs 409 dans les logs Logstash (dernières 100 lignes)
echo "=== Erreurs 409 (déduplication) dans les logs Logstash ==="
docker logs nexus-logstash --tail 100 2>&1 | grep -c "version_conflict_engine_exception" || echo "0"
echo ""

echo "=== Recommandations ==="
if [ ! -z "$PERCENT_IOT" ] && [ $(echo "$PERCENT_IOT > 5" | bc) -eq 1 ]; then
    echo "⚠️  Écart IoT trop élevé (>5%). Vérifiez le fingerprint dans 01-input-iot.conf"
elif [ ! -z "$PERCENT_IOT" ]; then
    echo "✅ Écart IoT acceptable (<5%)"
fi

if [ ! -z "$PERCENT_MAINT" ] && [ $(echo "$PERCENT_MAINT > 5" | bc) -eq 1 ]; then
    echo "⚠️  Écart Maintenance trop élevé (>5%). Vérifiez la configuration SFTP"
elif [ ! -z "$PERCENT_MAINT" ]; then
    echo "✅ Écart Maintenance acceptable (<5%)"
fi
