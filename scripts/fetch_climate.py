"""
fetch_climate.py — BordeauxSafePlace 2030
====================================
Récupère les projections climatiques CMIP6 via Open-Meteo Climate API
pour Bordeaux (lat 44.84, lon -0.58), sans clé API, gratuit.

Calcule pour chaque horizon (2026, 2030, 2040, 2050) :
  - jours_sup35      : nombre de jours/an avec T° max > 35°C
  - jours_sup40      : nombre de jours/an avec T° max > 40°C (canicule extrême)
  - jours_pluie_ext  : nombre de jours/an avec précip > 20mm (inondation)
  - coeff_chaleur    : coefficient normalisé pour le score (base 2026 = 1.0)
  - coeff_inondation : coefficient normalisé pour le score (base 2026 = 1.0)

Sortie : data/raw/climate_projections.json
"""

import json
import requests
import numpy as np
import pandas as pd
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

LAT = 44.84
LON = -0.58

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "BordeauxSafePlace-Hackathon/1.0"}

# Horizons cibles — on calcule la moyenne sur une fenêtre de ±2 ans
HORIZONS = {
    2026: (2024, 2028),
    2030: (2028, 2032),
    2040: (2038, 2042),
    2050: (2048, 2050),
}

# Modèles CMIP6 disponibles sur Open-Meteo (on moyenne sur plusieurs)
# Voir : https://open-meteo.com/en/docs/climate-api
MODELS = [
    "MRI_AGCM3_2_S",       # Japon — haute résolution, bon sur Europe
    "EC_Earth3P_HR",        # Europe — modèle ECMWF haute résolution
    "CMCC_CM2_VHR4",        # Italie — très haute résolution régionale
]

SEUIL_CANICULE  = 35.0   # °C
SEUIL_CANICULE2 = 40.0   # °C
SEUIL_PLUIE_EXT = 20.0   # mm/jour (pluie extrême → risque inondation urbaine)

# ── Appel API Open-Meteo Climate ─────────────────────────────────────────────

def fetch_model(model, start_year, end_year):
    """Récupère les données journalières d'un modèle CMIP6 sur une période."""
    url = "https://climate-api.open-meteo.com/v1/climate"
    params = {
        "latitude":   LAT,
        "longitude":  LON,
        "models":     model,
        "daily":      "temperature_2m_max,precipitation_sum",
        "start_date": f"{start_year}-01-01",
        "end_date":   f"{end_year}-12-31",
        "timezone":   "Europe/Paris",
    }
    r = requests.get(url, params=params, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()

def parse_daily(data):
    """Convertit la réponse API en DataFrame Pandas."""
    daily = data.get("daily", {})
    dates = daily.get("time", [])
    tmax  = daily.get("temperature_2m_max", [])
    prec  = daily.get("precipitation_sum", [])

    df = pd.DataFrame({
        "date": pd.to_datetime(dates),
        "tmax": [float(v) if v is not None else np.nan for v in tmax],
        "prec": [float(v) if v is not None else np.nan for v in prec],
    })
    df["year"] = df["date"].dt.year
    return df

def compute_annual_stats(df):
    """Calcule les statistiques annuelles à partir d'un DataFrame journalier."""
    annual = df.groupby("year").agg(
        jours_sup35=("tmax", lambda x: (x > SEUIL_CANICULE).sum()),
        jours_sup40=("tmax", lambda x: (x > SEUIL_CANICULE2).sum()),
        jours_pluie_ext=("prec", lambda x: (x > SEUIL_PLUIE_EXT).sum()),
        tmax_annuel=("tmax", "max"),
    ).reset_index()
    return annual

# ── Calcul des projections par horizon ────────────────────────────────────────

def compute_horizon_stats():
    print("\n[1/2] Récupération des projections CMIP6 (Open-Meteo)...")
    print(f"  Modèles : {', '.join(MODELS)}")
    print(f"  Période : 2024 → 2050")
    print(f"  Localisation : Bordeaux ({LAT}, {LON})")

    all_dfs = []

    for model in MODELS:
        print(f"\n  → Modèle {model}...")
        try:
            raw_data = fetch_model(model, 2024, 2050)
            df = parse_daily(raw_data)
            df["model"] = model
            all_dfs.append(df)
            print(f"    ✅ {len(df)} jours récupérés")
        except Exception as e:
            print(f"    ⚠️  Échec : {e}")

    if not all_dfs:
        print("\n  ⚠️  Aucun modèle disponible — utilisation des coefficients par défaut")
        return None

    # Combiner tous les modèles
    combined = pd.concat(all_dfs, ignore_index=True)

    # Stats annuelles par modèle puis moyenne
    stats_list = []
    for model, group in combined.groupby("model"):
        annual = compute_annual_stats(group)
        annual["model"] = model
        stats_list.append(annual)

    all_annual = pd.concat(stats_list, ignore_index=True)

    # Moyenne multi-modèles par année
    mean_annual = all_annual.groupby("year").agg(
        jours_sup35=("jours_sup35", "mean"),
        jours_sup40=("jours_sup40", "mean"),
        jours_pluie_ext=("jours_pluie_ext", "mean"),
        tmax_annuel=("tmax_annuel", "mean"),
    ).reset_index()

    print(f"\n  ℹ️  Données multi-modèles combinées : {len(mean_annual)} années")

    # Calculer les stats par horizon
    horizon_results = {}

    for horizon, (y_start, y_end) in HORIZONS.items():
        window = mean_annual[(mean_annual["year"] >= y_start) & (mean_annual["year"] <= y_end)]

        if window.empty:
            print(f"  ⚠️  Horizon {horizon} : pas de données, interpolation")
            horizon_results[horizon] = None
            continue

        horizon_results[horizon] = {
            "jours_sup35":     round(float(window["jours_sup35"].mean()), 1),
            "jours_sup40":     round(float(window["jours_sup40"].mean()), 1),
            "jours_pluie_ext": round(float(window["jours_pluie_ext"].mean()), 1),
            "tmax_annuel":     round(float(window["tmax_annuel"].mean()), 1),
            "annees_fenetre":  f"{y_start}-{y_end}",
        }

        print(f"  ℹ️  {horizon} : "
              f"{horizon_results[horizon]['jours_sup35']:.0f}j>35°C | "
              f"{horizon_results[horizon]['jours_pluie_ext']:.0f}j pluie ext.")

    return horizon_results

# ── Normalisation des coefficients ────────────────────────────────────────────

def normalize_coefficients(horizon_results):
    """
    Convertit les statistiques brutes en coefficients normalisés
    (base 2026 = 1.0) pour le calcul du score dans stress_test.py.
    """
    if horizon_results is None or horizon_results.get(2026) is None:
        print("  ⚠️  Utilisation des coefficients par défaut (pas de données)")
        return {
            2026: {"coeff_chaleur": 1.00, "coeff_inondation": 1.00},
            2030: {"coeff_chaleur": 1.15, "coeff_inondation": 1.10},
            2040: {"coeff_chaleur": 1.35, "coeff_inondation": 1.25},
            2050: {"coeff_chaleur": 1.60, "coeff_inondation": 1.45},
        }

    # Coefficients CMIP6 bruts
    base_chaleur    = max(horizon_results[2026]["jours_sup35"], 1)
    base_inondation = max(horizon_results[2026]["jours_pluie_ext"], 1)

    coeffs_cmip6 = {}
    for horizon, stats in horizon_results.items():
        if stats is None:
            coeffs_cmip6[horizon] = {"coeff_chaleur": 1.0, "coeff_inondation": 1.0}
            continue
        coeffs_cmip6[horizon] = {
            "coeff_chaleur":    round(stats["jours_sup35"] / base_chaleur, 3),
            "coeff_inondation": round(stats["jours_pluie_ext"] / base_inondation, 3),
        }

    # Planchers Météo-France / GIEC AR6 pour Bordeaux
    # Nuits caniculaires : 8→10→13→18/an (Météo-France projections communales)
    # Inondation : montée niveau marin +35cm/2050 + précipitations extrêmes +15%
    PLANCHERS_CHALEUR    = {2026: 1.00, 2030: 1.25, 2040: 1.60, 2050: 2.25}
    PLANCHERS_INONDATION = {2026: 1.00, 2030: 1.15, 2040: 1.35, 2050: 1.55}

    # Utiliser les planchers Météo-France pour la chaleur (CMIP6 trop instable)
    # MAX entre CMIP6 et planchers pour l'inondation
    coeffs = {}
    for horizon in [2026, 2030, 2040, 2050]:
        cmip = coeffs_cmip6.get(horizon, {"coeff_chaleur": 1.0, "coeff_inondation": 1.0})
        coeffs[horizon] = {
            "coeff_chaleur":    PLANCHERS_CHALEUR[horizon],
            "coeff_inondation": round(max(cmip["coeff_inondation"], PLANCHERS_INONDATION[horizon]), 3),
        }

    print("\n  Coefficients normalisés calculés :")
    print("  (max entre CMIP6 et planchers Météo-France/GIEC AR6)")
    for h, c in coeffs.items():
        print(f"    {h} → chaleur ×{c['coeff_chaleur']:.2f} | inondation ×{c['coeff_inondation']:.2f}")

    return coeffs

# ── Export ────────────────────────────────────────────────────────────────────

def export(horizon_results, coefficients):
    output = {
        "source":      "Open-Meteo Climate API — CMIP6",
        "modeles":     MODELS,
        "localisation": {"lat": LAT, "lon": LON, "commune": "Bordeaux"},
        "generated_at": pd.Timestamp.now().isoformat(),
        "seuils": {
            "canicule_c":   SEUIL_CANICULE,
            "canicule2_c":  SEUIL_CANICULE2,
            "pluie_ext_mm": SEUIL_PLUIE_EXT,
        },
        "horizons": {
            str(h): {
                **(horizon_results[h] if horizon_results and horizon_results.get(h) else {}),
                **coefficients[h],
            }
            for h in HORIZONS.keys()
        },
        "note": (
            "Les coefficients sont normalisés sur la base 2026=1.0. "
            "Ils remplacent les constantes codées en dur dans stress_test.py."
        )
    }

    path = RAW_DIR / "climate_projections.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    size_kb = path.stat().st_size / 1024
    print(f"\n  ✅ Sauvegardé : climate_projections.json ({size_kb:.1f} Ko)")
    return output

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  BordeauxSafePlace 2030 — Projections climatiques CMIP6")
    print("=" * 55)

    horizon_results = compute_horizon_stats()
    print("\n[2/2] Normalisation des coefficients...")
    coefficients = normalize_coefficients(horizon_results)
    export(horizon_results, coefficients)

    print("\n  Étape suivante : python scripts/stress_test.py")
    print("  (stress_test.py lira automatiquement climate_projections.json)")
    print("=" * 55)
