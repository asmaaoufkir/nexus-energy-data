import sys, json

try:
    data = json.load(sys.stdin).get("indices", {})
except Exception as e:
    print(f"Erreur de lecture JSON: {e}")
    sys.exit(1)

def get_stats(idx):
    obj = data.get(idx, {}).get("primaries", {})
    docs = obj.get("docs", {}).get("count", 0)
    bytes_val = obj.get("store", {}).get("size_in_bytes", 0)
    return docs, bytes_val

c_docs, c_bytes = get_stats("metrics-energy-iot-classic")
h_docs, h_bytes = get_stats(".ds-metrics-energy.iot-default-2026.08.10-000001")
w_docs, w_bytes = get_stats("downsampled-metrics-energy-iot-1h")

def fmt_docs(d, is_warm=False):
    if is_warm:
        return f"{d} agrégats"
    if d >= 1_000_000:
        return f"~{d / 1_000_000:.1f} Millions"
    return f"{d}"

def fmt_size(b):
    if b >= 1024**3:
        return f"{b / (1024**3):.2f} Go"
    elif b >= 1024**2:
        return f"{b / (1024**2):.1f} Mo"
    elif b >= 1024:
        return f"{b / 1024:.1f} Ko"
    return f"{b} B"

c_opd = c_bytes / c_docs if c_docs else 0
h_opd = h_bytes / h_docs if h_docs else 0

gain_hot = ((h_opd - c_opd) / c_opd * 100) if c_opd else 0
gain_warm = ((w_bytes - h_bytes) / h_bytes * 100) if h_bytes else -99.99

print("\nBILAN BENCHMARK : NEXUS ENERGY DATA PIPELINE")
print("=" * 105)
print(f"{'Indicateur':<25} | {'Index Classique':<22} | {'TSDS Optimisé (Hot)':<24} | {'TSDS Downsamplé (Warm)':<25}")
print("-" * 105)
print(f"{'Volume de Documents':<25} | {fmt_docs(c_docs):<22} | {fmt_docs(h_docs):<24} | {fmt_docs(w_docs, True):<25}")
print(f"{'Taille Disque Globale':<25} | {fmt_size(c_bytes):<22} | {fmt_size(h_bytes):<24} | {fmt_size(w_bytes):<25}")
print(f"{'Empreinte / Document':<25} | {f'~{int(c_opd)} octets / doc':<22} | {f'~{int(h_opd)} octets / doc':<24} | {'N/A (Séries temporelles)':<25}")
print(f"{'Gain de Stockage Disque':<25} | {'Baseline (0%)':<22} | {f'{gain_hot:.1f} %':<24} | {f'{gain_warm:.2f} %':<25}")
print(f"{'Précision Temporelle':<25} | {'Granularité brute':<22} | {'Granularité brute':<24} | {'Agrégats 1h (min/max/avg)':<25}")
print("=" * 105 + "\n")
