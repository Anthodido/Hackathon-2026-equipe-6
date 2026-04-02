"""
fetch_data.py — BordeauxSafePlace v3
=====================================
IDs corrigés suite aux recherches sur le DataHub :
  - Îlots de chaleur  : ri_icu_ifu_s (trouvé sur DataHub)
  - Vulnérabilité ch. : ri_vulnerabilite_s
  - PPRI              : via Pigma / Géorisques (pas sur DataHub public)
  - Équipements       : to_eqpub_p (filtre Python après DL complet)
  - Lieux frais       : plusieurs IDs testés

Sortie : data/raw/
"""

import json
import time
import requests
import pandas as pd
from pathlib import Path

CODE_INSEE  = "33063"
COMMUNE_NOM = "Bordeaux"

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "BordeauxSafePlace-Hackathon/1.0", "Accept": "application/json"}

# ── Helpers ───────────────────────────────────────────────────────────────────

def save_json(data, filename):
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    size_kb = path.stat().st_size / 1024
    print(f"    ✅ {filename} ({size_kb:.1f} Ko)")
    return path

def datahub_get(dataset_id, filename, where=None, label=None, select=None, fmt="geojson"):
    """Appel DataHub v2.1 — essaie les deux domaines."""
    bases = [
        "https://datahub.bordeaux-metropole.fr/api/explore/v2.1/catalog/datasets",
        "https://opendata.bordeaux-metropole.fr/api/explore/v2.1/catalog/datasets",
    ]
    label = label or dataset_id
    print(f"  → {label}...")

    for base in bases:
        url = f"{base}/{dataset_id}/exports/{fmt}"
        params = {"limit": -1, "timezone": "Europe/Paris"}
        if where:
            params["where"] = where
        if select:
            params["select"] = select
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=60)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            data = r.json()
            if fmt == "geojson":
                nb = len(data.get("features", []))
                if nb == 0:
                    continue
                save_json(data, filename)
                print(f"    ℹ️  {nb} features")
            else:
                if not data:
                    continue
                save_json(data, filename)
                print(f"    ℹ️  {len(data)} enregistrements")
            return data
        except requests.HTTPError as e:
            code = e.response.status_code if e.response else "?"
            print(f"    ⚠️  HTTP {code} sur {base.split('/')[2]}")
            continue
        except Exception as e:
            print(f"    ⚠️  {e}")
            continue
    return None

def filter_bordeaux(geojson, filename):
    """Filtre un GeoJSON sur Bordeaux avec auto-détection du champ commune."""
    if not geojson or not geojson.get("features"):
        return None

    sample = geojson["features"][0].get("properties", {})
    print(f"    ℹ️  Champs : {list(sample.keys())[:12]}")

    # Auto-détecter le champ qui contient "bordeaux"
    CANDIDATES = [
        "COMMUNE", "commune", "Commune", "NOM_COM", "nom_com",
        "libelle_commune", "NOM_COMMUNE", "nom_commune",
        "LIB_COM", "lib_com", "LIBELLE", "libelle", "city", "ville",
    ]
    detected = None
    for feat in geojson["features"][:100]:
        props = feat.get("properties", {})
        for field in CANDIDATES:
            if "bordeaux" in str(props.get(field, "")).lower():
                detected = field
                print(f"    ℹ️  Champ commune : '{field}' = '{props[field]}'")
                break
        if detected:
            break

    # Scan complet si pas trouvé via liste
    if not detected:
        for feat in geojson["features"][:20]:
            for k, v in feat.get("properties", {}).items():
                if "bordeaux" in str(v).lower():
                    detected = k
                    print(f"    ℹ️  Champ détecté (scan) : '{k}' = '{v}'")
                    break
            if detected:
                break

    if not detected:
        print("    ⚠️  Champ commune introuvable — toutes les features conservées")
        save_json(geojson, filename)
        return geojson

    features = [
        f for f in geojson["features"]
        if "bordeaux" in str(f.get("properties", {}).get(detected, "")).lower()
    ]
    result = {"type": "FeatureCollection", "features": features}
    save_json(result, filename)
    print(f"    ℹ️  {len(features)}/{len(geojson['features'])} features Bordeaux")
    return result

# ── 1. Équipements publics ────────────────────────────────────────────────────

def fetch_equipements():
    print("\n[1/5] Équipements publics...")

    raw = datahub_get(
        dataset_id="to_eqpub_p",
        filename="_eqpub_all.geojson",
        label="Équipements publics (métropole complète)"
    )
    if raw:
        # Filtrer par bbox Bordeaux (plus fiable que filtre sur champ commune)
        # Bordeaux : lat 44.80-44.90, lon -0.65 à -0.52
        BBOX = {"lat_min": 44.80, "lat_max": 44.90, "lon_min": -0.65, "lon_max": -0.52}
        features = []
        for f in raw.get("features", []):
            try:
                coords = f.get("geometry", {}).get("coordinates", [])
                if not coords:
                    # Essayer geo_point_2d dans properties
                    geo = f.get("properties", {}).get("geo_point_2d", {})
                    lon = float(geo.get("lon", 0))
                    lat = float(geo.get("lat", 0))
                else:
                    lon, lat = float(coords[0]), float(coords[1])
                if (BBOX["lat_min"] <= lat <= BBOX["lat_max"] and
                    BBOX["lon_min"] <= lon <= BBOX["lon_max"]):
                    features.append(f)
            except (TypeError, ValueError, KeyError):
                continue

        result = {"type": "FeatureCollection", "features": features}
        save_json(result, "equipements_publics_bordeaux.geojson")
        print(f"    ℹ️  {len(features)}/{len(raw.get('features',[]))} features dans bbox Bordeaux")

        # Afficher la distribution des thèmes
        from collections import Counter
        themes = Counter(f["properties"].get("theme","?") for f in features)
        print(f"    ℹ️  Thèmes : {dict(themes)}")

        tmp = RAW_DIR / "_eqpub_all.geojson"
        if tmp.exists():
            tmp.unlink()
    else:
        print("    ❌ Équipements non disponibles")

# ── 2. Réservations salles ────────────────────────────────────────────────────

def fetch_salles_capacites():
    print("\n[2/5] Capacités salles municipales...")

    result = datahub_get(
        dataset_id="bor_reservations_salles_municipales_donnees",
        filename="salles_capacites.json",
        label="Réservations salles municipales",
        fmt="json"
    )
    if result is None:
        print("    ℹ️  Salles non disponibles — fichier vide créé")
        save_json([], "salles_capacites.json")

# ── 3. PPRI zones inondables ──────────────────────────────────────────────────

def fetch_ppri():
    print("\n[3/5] Zones inondables PPRI...")

    # Géorisques — plusieurs endpoints possibles
    geo_endpoints = [
        f"https://georisques.gouv.fr/api/v1/gaspar/ppr?codeCommune={CODE_INSEE}&formatRetour=geojson",
        f"https://georisques.gouv.fr/api/v1/zonages_ppr?code_commune={CODE_INSEE}&format=geojson",
    ]
    for url in geo_endpoints:
        print(f"  → Géorisques...")
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code in (400, 404, 500):
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("features"):
                save_json(data, "zones_inondables.geojson")
                print(f"    ℹ️  {len(data['features'])} zones")
                return
        except Exception as e:
            print(f"    ⚠️  {e}")

    # DataHub BM — IDs possibles
    for did in ["ri_ppri_zone_s", "ri_inondation_s", "ri_alea_inondation_s"]:
        result = datahub_get(
            dataset_id=did,
            filename="zones_inondables.geojson",
            label=f"PPRI DataHub ({did})"
        )
        if result:
            return

    # Fallback — guide téléchargement manuel
    print("    ❌ PPRI non récupérable automatiquement")
    print()
    print("    ╔══ TÉLÉCHARGEMENT MANUEL REQUIS ════════════════════╗")
    print("    ║  1. https://www.georisques.gouv.fr                 ║")
    print("    ║     → Carte interactive → Inondation → Export JSON ║")
    print("    ║  OU                                                 ║")
    print("    ║  2. https://datahub.bordeaux-metropole.fr          ║")
    print("    ║     → chercher 'ppri' → Export GeoJSON             ║")
    print("    ║  → Sauvegarder : data/raw/zones_inondables.geojson ║")
    print("    ╚════════════════════════════════════════════════════╝")

# ── 4. Îlots de chaleur ───────────────────────────────────────────────────────

def fetch_ilots_chaleur():
    print("\n[4/5] Îlots de chaleur...")

    # ri_icu_ifu_s = "Ilot de chaleur ou de fraicheur urbain basé sur
    # l'analyse des températures de surface de 2022" — trouvé sur DataHub
    ids_candidates = [
        ("ri_icu_ifu_s",       "Îlots chaleur/fraîcheur 2022 ✓ trouvé sur DataHub"),
        ("ri_vulnerabilite_s", "Vulnérabilité chaleur ✓ trouvé sur DataHub"),
        ("en_icu_s",           "Îlots chaleur (en_icu_s)"),
        ("ri_icu_s",           "Îlots chaleur (ri_icu_s)"),
    ]

    for did, label in ids_candidates:
        result = datahub_get(
            dataset_id=did,
            filename="ilots_chaleur.geojson",
            label=label
        )
        if result:
            return

    print("    ❌ Îlots de chaleur non disponibles via API")
    print("    📥 https://datahub.bordeaux-metropole.fr/explore/dataset/ri_icu_ifu_s/")
    print("       → Export → GeoJSON → data/raw/ilots_chaleur.geojson")

# ── Résumé ────────────────────────────────────────────────────────────────────

def print_summary():
    fichiers = [
        ("equipements_publics_bordeaux.geojson", "Équipements"),
        ("salles_capacites.json",                "Capacités salles"),
        ("zones_inondables.geojson",             "PPRI inondation"),
        ("ilots_chaleur.geojson",                "Îlots chaleur + fraîcheur"),
        ("catnat_bordeaux.json",                 "CATNAT historique"),
        ("azi_bordeaux.geojson",                 "AZI zones inondables"),
    ]
    print("\n" + "=" * 55)
    ok = 0
    for fname, label in fichiers:
        path = RAW_DIR / fname
        if path.exists() and path.stat().st_size > 100:
            size_kb = path.stat().st_size / 1024
            print(f"  ✅ {label:<26} ({size_kb:.0f} Ko)")
            ok += 1
        else:
            print(f"  ❌ {label:<26} manquant ou vide")

    print(f"\n  {ok}/{len(fichiers)} fichiers disponibles")
    if ok >= 3:
        print("\n  → python fetch_climate.py")
        print("  → python stress_test.py")
    print("=" * 55)


# ── 5. CATNAT (historique catastrophes naturelles) ────────────────────────────

def fetch_catnat():
    """
    Récupère l'historique des catastrophes naturelles (inondations) 
    pour Bordeaux via l'API Géorisques.
    URL corrigée : code_insee au lieu de longitude/latitude/rayon
    """
    print("\n[5/6] CATNAT — Historique inondations Bordeaux...")

    url = f"https://georisques.gouv.fr/api/v1/gaspar/catnat?code_insee={CODE_INSEE}&page_size=100"
    print(f"  → API Géorisques CATNAT...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        evenements = data.get("data", [])

        # Filtrer les inondations
        inondations = [
            e for e in evenements
            if "inond" in (e.get("libelle_risque_jo", "") or "").lower()
            or "submersion" in (e.get("libelle_risque_jo", "") or "").lower()
            or "coulée" in (e.get("libelle_risque_jo", "") or "").lower()
        ]

        # Sauvegarder en JSON simple (pas GeoJSON — pas de géométrie)
        result = {
            "total": len(inondations),
            "commune": "Bordeaux",
            "code_insee": CODE_INSEE,
            "evenements": [
                {
                    "date_debut": e.get("date_debut_evt", ""),
                    "date_fin":   e.get("date_fin_evt", ""),
                    "libelle":    e.get("libelle_risque_jo", ""),
                    "arrete":     e.get("date_publication_arrete", ""),
                }
                for e in inondations
            ]
        }

        save_json(result, "catnat_bordeaux.json")
        print(f"    ℹ️  {len(inondations)} inondations recensées depuis 1982")

        # Afficher les années
        annees = sorted(set(
            e["date_debut"][-4:] for e in result["evenements"] 
            if e["date_debut"] and len(e["date_debut"]) >= 4
        ))
        print(f"    ℹ️  Années : {', '.join(annees)}")
        return result

    except Exception as e:
        print(f"    ⚠️  {e}")
        print("    ❌ CATNAT non disponible")
        return None


# ── 6. AZI (Atlas Zones Inondables) ──────────────────────────────────────────

def fetch_azi():
    """
    Récupère l'Atlas des Zones Inondables via Géorisques.
    Complément au PPRI pour les zones non couvertes.
    URL corrigée : code_insee au lieu de longitude/latitude/rayon
    """
    print("\n[6/6] AZI — Atlas Zones Inondables...")

    # L'AZI n'a pas d'endpoint par code_insee — on utilise bbox Bordeaux
    url = (
        "https://georisques.gouv.fr/api/v1/azi"
        "?latmin=44.80&latmax=44.90&lonmin=-0.65&lonmax=-0.52"
        "&page=1&page_size=500"
    )
    print(f"  → API Géorisques AZI...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 404:
            print("    ⚠️  AZI endpoint non disponible (404)")
            return None
        r.raise_for_status()
        data = r.json()
        zones = data.get("data", [])

        if not zones:
            print("    ⚠️  Aucune zone AZI trouvée")
            return None

        # Convertir en GeoJSON
        features = []
        for z in zones:
            geom = z.get("geom")
            if not geom:
                continue
            alea = z.get("lib_alea", z.get("alea", "moyen")) or "moyen"
            a = alea.lower()
            statut = "critique" if ("fort" in a or "tres" in a) else ("menace" if "moyen" in a else "sur")
            features.append({
                "type": "Feature",
                "properties": {
                    "id":     z.get("id", ""),
                    "alea":   alea,
                    "statut": statut,
                    "type_risque": "inondation_azi"
                },
                "geometry": geom
            })

        geojson = {"type": "FeatureCollection", "features": features}
        save_json(geojson, "azi_bordeaux.geojson")
        print(f"    ℹ️  {len(features)} zones AZI exportées")
        return geojson

    except Exception as e:
        print(f"    ⚠️  {e}")
        print("    ❌ AZI non disponible")
        return None

# ── Main ──────────────────────────────────────────────────────────────────────

def already_exists(filename):
    """Retourne True si le fichier existe déjà et n'est pas vide."""
    path = RAW_DIR / filename
    if path.exists() and path.stat().st_size > 100:
        size_kb = path.stat().st_size / 1024
        print(f"\n  ⏭️  {filename} déjà présent ({size_kb:.0f} Ko) — ignoré")
        return True
    return False

if __name__ == "__main__":
    print("=" * 55)
    print("  BordeauxSafePlace — Téléchargement des données v3")
    print("=" * 55)
    print(f"  Commune : {COMMUNE_NOM} (INSEE {CODE_INSEE})")
    print("  ℹ️  Les fichiers déjà présents dans data/raw/ ne seront pas écrasés")

    if not already_exists("equipements_publics_bordeaux.geojson"):
        fetch_equipements()
        time.sleep(1)

    if not already_exists("salles_capacites.json"):
        fetch_salles_capacites()
        time.sleep(1)

    if not already_exists("zones_inondables.geojson"):
        fetch_ppri()
        time.sleep(1)

    if not already_exists("ilots_chaleur.geojson"):
        fetch_ilots_chaleur()
        time.sleep(1)

    if not already_exists("catnat_bordeaux.json"):
        fetch_catnat()
        time.sleep(1)

    if not already_exists("azi_bordeaux.geojson"):
        fetch_azi()
        time.sleep(1)

    print_summary()