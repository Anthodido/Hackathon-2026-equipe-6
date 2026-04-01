"""
fetch_data.py  —  Dorian / appels API  —  Sentinelle 2030
Utilisation :
    source venv/bin/activate
    pip install -r requirements.txt
    python scripts/fetch_data.py
"""

import requests
import json
import os

os.makedirs("data/processed", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)

LONGITUDE = -0.5792
LATITUDE  = 44.8378
RAYON     = 8000


def dans_bbox_bordeaux(geometry):
    if not geometry:
        return False
    try:
        coords = geometry.get("coordinates", [])
        t = geometry["type"]
        if t == "Point":
            lon, lat = coords[0], coords[1]
        elif t == "Polygon":
            lon, lat = coords[0][0][0], coords[0][0][1]
        elif t == "MultiPolygon":
            lon, lat = coords[0][0][0][0], coords[0][0][0][1]
        else:
            return True
        return -0.612 <= lon <= -0.534 and 44.808 <= lat <= 44.887
    except (IndexError, TypeError, KeyError):
        return True


# ── 1. CATNAT (catastrophes naturelles inondation) ────────────────────────────
print("=" * 60)
print("1. API Géorisques — CATNAT inondation")
print("=" * 60)

URL_CATNAT = (
    f"https://georisques.gouv.fr/api/v1/gaspar/catnat/"
    f"?longitude={LONGITUDE}&latitude={LATITUDE}&rayon={RAYON}"
    f"&page=1&page_size=100"
)
print(f"URL : {URL_CATNAT}\n")

try:
    r = requests.get(URL_CATNAT, timeout=15)
    r.raise_for_status()
    brut = r.json()

    with open("data/raw/catnat_raw.json", "w", encoding="utf-8") as f:
        json.dump(brut, f, ensure_ascii=False, indent=2)

    evenements = brut.get("data", brut if isinstance(brut, list) else [])
    print(f"OK : {len(evenements)} evenements catnat trouves")

    inondations = [
        e for e in evenements
        if "inond" in (e.get("libelle_risque_jo", "") or "").lower()
        or "submersion" in (e.get("libelle_risque_jo", "") or "").lower()
    ]
    print(f"   -> {len(inondations)} inondations filtrees")

    geojson = {"type": "FeatureCollection", "name": "zones_inondables_bordeaux", "features": []}
    for e in inondations:
        libelle = e.get("libelle_risque_jo", "Inondation")
        l = libelle.lower()
        statut = "critique" if ("inond" in l or "submersion" in l) else "menace"
        feature = {
            "type": "Feature",
            "properties": {
                "id": e.get("num_risque", ""),
                "nom_risque": libelle,
                "date_debut": e.get("dat_deb", ""),
                "commune": e.get("lib_commune", "Bordeaux"),
                "statut": statut,
                "type_risque": "inondation"
            },
            "geometry": e.get("geom") or {"type": "Point", "coordinates": [LONGITUDE, LATITUDE]}
        }
        geojson["features"].append(feature)

    with open("data/processed/zones_inondables.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    print(f"Exporte -> data/processed/zones_inondables.geojson ({len(geojson['features'])} features)\n")

except Exception as e:
    print(f"ERREUR : {e}\n")


# ── 2. AZI (Atlas Zones Inondables) ──────────────────────────────────────────
print("=" * 60)
print("2. API Géorisques — AZI")
print("=" * 60)

URL_AZI = (
    f"https://georisques.gouv.fr/api/v1/azi"
    f"?longitude={LONGITUDE}&latitude={LATITUDE}&rayon={RAYON}"
    f"&page=1&page_size=100"
)
print(f"URL : {URL_AZI}\n")

try:
    r2 = requests.get(URL_AZI, timeout=15)
    r2.raise_for_status()
    brut2 = r2.json()

    with open("data/raw/azi_raw.json", "w", encoding="utf-8") as f:
        json.dump(brut2, f, ensure_ascii=False, indent=2)

    zones_azi = brut2.get("data", brut2 if isinstance(brut2, list) else [])
    print(f"OK : {len(zones_azi)} zones AZI trouvees")

    geojson_azi = {"type": "FeatureCollection", "name": "azi_bordeaux", "features": []}
    for z in zones_azi:
        alea = z.get("alea", z.get("lib_alea", "moyen"))
        a = (alea or "").lower()
        statut = "critique" if ("fort" in a or "tres" in a) else ("menace" if "moyen" in a else "sur")
        feature = {
            "type": "Feature",
            "properties": {
                "id": z.get("id", ""),
                "nom": z.get("lib_alea", "Zone inondable"),
                "alea": alea,
                "statut": statut,
                "type_risque": "inondation"
            },
            "geometry": z.get("geom", None)
        }
        geojson_azi["features"].append(feature)

    with open("data/processed/azi_bordeaux.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson_azi, f, ensure_ascii=False, indent=2)
    print(f"Exporte -> data/processed/azi_bordeaux.geojson\n")

except Exception as e:
    print(f"ERREUR AZI : {e}\n")


# ── 3. ÎLOTS DE CHALEUR — Bordeaux Métropole ─────────────────────────────────
print("=" * 60)
print("3. Open data Bordeaux Metropole — ilots de chaleur")
print("=" * 60)

URL_CHALEUR = "https://data.bordeaux-metropole.fr/geojson/features/EC_ILOT_CHALEUR_S"
print(f"URL : {URL_CHALEUR}\n")

try:
    r3 = requests.get(URL_CHALEUR, timeout=30)
    r3.raise_for_status()
    data_chaleur = r3.json()

    with open("data/raw/ilots_chaleur_raw.geojson", "w", encoding="utf-8") as f:
        json.dump(data_chaleur, f, ensure_ascii=False, indent=2)

    features_tous = data_chaleur.get("features", [])
    print(f"OK : {len(features_tous)} ilots recuperes")

    features_bordeaux = [f for f in features_tous if dans_bbox_bordeaux(f.get("geometry"))]
    print(f"   -> {len(features_bordeaux)} dans la commune de Bordeaux")

    for feat in features_bordeaux:
        props = feat.get("properties", {})
        cat = (props.get("CATEGORIE") or props.get("categorie") or props.get("TYPE_ICU") or props.get("CLASSE") or "")
        c = cat.lower()
        if "fort" in c or "intense" in c or "tres" in c or "eleve" in c:
            statut = "critique"
        elif "moyen" in c or "modere" in c:
            statut = "menace"
        else:
            statut = "sur"
        feat["properties"]["statut"] = statut
        feat["properties"]["type_risque"] = "chaleur"

    geojson_chaleur = {"type": "FeatureCollection", "name": "ilots_chaleur_bordeaux", "features": features_bordeaux}

    with open("data/processed/ilots_chaleur.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson_chaleur, f, ensure_ascii=False, indent=2)

    critiques = sum(1 for f in features_bordeaux if f["properties"]["statut"] == "critique")
    menaces   = sum(1 for f in features_bordeaux if f["properties"]["statut"] == "menace")
    print(f"   Critiques : {critiques} | Menaces : {menaces}")
    print(f"Exporte -> data/processed/ilots_chaleur.geojson\n")

except Exception as e:
    print(f"ERREUR : {e}\n")


# ── Résumé ────────────────────────────────────────────────────────────────────
print("=" * 60)
print("RESUME — fichiers dans data/processed/")
print("=" * 60)
for nom in ["zones_inondables.geojson", "azi_bordeaux.geojson", "ilots_chaleur.geojson"]:
    chemin = f"data/processed/{nom}"
    if os.path.exists(chemin):
        taille = os.path.getsize(chemin)
        print(f"  OK  {nom} ({taille:,} octets)")
    else:
        print(f"  MANQUANT  {nom}")
