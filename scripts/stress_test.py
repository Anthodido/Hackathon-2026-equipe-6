"""
stress_test.py — BordeauxSafePlace
==================================
Croise les équipements (gymnases, salles) avec les zones de risque
(inondation PPRI, îlots de chaleur) pour calculer un score de pérennité
par horizon temporel (2026, 2030, 2040, 2050).

Entrée  : data/raw/
Sortie  : data/processed/refuges_diagnostiques.json
"""

import json
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path

warnings.filterwarnings("ignore")

# ── Chemins ───────────────────────────────────────────────────────────────────

BASE_DIR  = Path(__file__).parent.parent
RAW_DIR   = BASE_DIR / "data" / "raw"
PROC_DIR  = BASE_DIR / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

# ── Thèmes à conserver comme refuges potentiels ───────────────────────────────
# Le dataset to_eqpub_p code les thèmes en lettres :
#   A=Enseignement  B=Santé  C=Sport-loisir  D=Culture
#   E=Administration  F=Sécurité  G=Espace vert  H=Sénior  I=Petite enfance
THEMES_CODES = ["B", "C", "G", "H", "I"]

# CARE officiels connus (activés lors de la crise de février 2026)
# Ces équipements ne sont pas dans to_eqpub_p — ajoutés manuellement
CARE_OFFICIELS = [
    "gymnase jean-dauguet",
    "salle eugenie eboue-tell",
    "salle eboue-tell",
    "gymnase promis",
]

# CARE manuels à injecter dans le JSON final (hors dataset)
CARE_MANUELS = [
    {
        "id": 9001,
        "nom": "Gymnase Jean-Dauguet",
        "type": "C",
        "adresse": "15 rue Ferdinand Palau",
        "commune": "Bordeaux",
        "quartier": "Bastide",
        "lat": 44.8431,   # coordonnées OSM exactes
        "lng": -0.5442,
        "superficie": 1132,
        "capacite": 2309,
        "care_officiel": True,
        "note": "CARE officiel activé lors de la crise de février 2026 — rive droite",
    },
    {
        "id": 9002,
        "nom": "Salle Eugénie Eboué-Tell",
        "type": "C",
        "adresse": "15-19 rue Marcel Pagnol",
        "commune": "Bordeaux",
        "quartier": "Bassins à Flot",
        "lat": 44.8638,   # Bassins à Flot, inauguré janvier 2026
        "lng": -0.5712,
        "superficie": 600,
        "capacite": 250,
        "care_officiel": True,
        "note": "CARE officiel activé lors de la crise de février 2026 — Bassins à Flot",
    },
    {
        "id": 9003,
        "nom": "Gymnase Promis",
        "type": "C",
        "adresse": "44 rue Promis",
        "commune": "Bordeaux",
        "quartier": "Bastide",
        "lat": 44.8396,   # coordonnées OSM exactes
        "lng": -0.5543,
        "superficie": 500,
        "capacite": 100,
        "care_officiel": True,
        "note": "CARE officiel — personnes sans abri — rive droite",
    },
]

# ── Pondérations du score par horizon ────────────────────────────────────────
# Score de pérennité : 0 (critique) → 100 (sûr)
# Chargées dynamiquement depuis climate_projections.json si disponible,
# sinon valeurs par défaut (constantes).

DEFAULT_HORIZONS = {
    2026: {"flood_weight": 1.0,  "heat_weight": 1.0},
    2030: {"flood_weight": 1.10, "heat_weight": 1.15},
    2040: {"flood_weight": 1.25, "heat_weight": 1.35},
    2050: {"flood_weight": 1.45, "heat_weight": 1.60},
}

def load_climate_horizons():
    """
    Charge les coefficients depuis climate_projections.json (produit par fetch_climate.py).
    Si le fichier n'existe pas, utilise les valeurs par défaut.
    """
    climate_path = RAW_DIR / "climate_projections.json"
    if not climate_path.exists():
        print("  ℹ️  climate_projections.json non trouvé — coefficients par défaut utilisés")
        print("      → Lancer fetch_climate.py pour des projections CMIP6 réelles")
        return DEFAULT_HORIZONS

    try:
        with open(climate_path, encoding="utf-8") as f:
            data = json.load(f)

        horizons_data = data.get("horizons", {})
        result = {}

        for h in [2026, 2030, 2040, 2050]:
            h_data = horizons_data.get(str(h), {})
            coeff_chaleur    = h_data.get("coeff_chaleur",    DEFAULT_HORIZONS[h]["heat_weight"])
            coeff_inondation = h_data.get("coeff_inondation", DEFAULT_HORIZONS[h]["flood_weight"])
            # Garantir une progression minimale entre horizons (plancher)
            # Les modèles CMIP6 peuvent sous-estimer la tendance sur courte période
            planchers_chaleur    = {2026: 1.0, 2030: 1.40, 2040: 1.80, 2050: 2.30}
            planchers_inondation = {2026: 1.0, 2030: 1.10, 2040: 1.30, 2050: 1.55}
            result[h] = {
                "flood_weight": max(coeff_inondation, planchers_inondation[h]),
                "heat_weight":  max(coeff_chaleur,    planchers_chaleur[h]),
            }

        source = data.get("source", "inconnu")
        print(f"  ✅ Projections climatiques chargées ({source})")
        for h, v in result.items():
            print(f"     {h} → chaleur ×{v['heat_weight']:.2f} | inondation ×{v['flood_weight']:.2f}")

        return result

    except Exception as e:
        print(f"  ⚠️  Erreur lecture climate_projections.json : {e}")
        print("      → Coefficients par défaut utilisés")
        return DEFAULT_HORIZONS

HORIZONS = load_climate_horizons()

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_geojson(filename, label):
    path = RAW_DIR / filename
    if not path.exists():
        print(f"  ⚠️  Fichier manquant : {filename} — ignoré")
        return None
    try:
        gdf = gpd.read_file(path)
        print(f"  ✅ {label} : {len(gdf)} features chargées")
        return gdf
    except Exception as e:
        print(f"  ⚠️  Erreur lecture {filename} : {e}")
        return None

def normalize_crs(gdf, target_epsg=4326):
    if gdf is None or gdf.empty:
        return gdf
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != target_epsg:
        gdf = gdf.to_crs(epsg=target_epsg)
    return gdf

def is_care_officiel(nom):
    if not nom:
        return False
    return any(c in nom.lower() for c in CARE_OFFICIELS)

# ── Étape 1 : Charger les données ─────────────────────────────────────────────

def load_all():
    print("\n[1/4] Chargement des données...")
    equipements = load_geojson("equipements_publics_bordeaux.geojson", "Équipements publics")
    ppri        = load_geojson("zones_inondables.geojson", "PPRI zones inondables")
    chaleur     = load_geojson("ilots_chaleur.geojson", "Îlots de chaleur")
    lieux_frais = load_geojson("ilots_chaleur.geojson", "Îlots chaleur/fraîcheur")

    # Capacités salles (JSON plat, pas GeoJSON)
    capacites = {}
    cap_path = RAW_DIR / "salles_capacites.json"
    if cap_path.exists():
        with open(cap_path, encoding="utf-8") as f:
            cap_data = json.load(f)
        for item in cap_data:
            nom = str(item.get("nom_equipement", "")).lower().strip()
            if nom:
                capacites[nom] = {
                    "superficie": item.get("superficie"),
                    "capacite":   item.get("capacite"),
                    "quartier":   item.get("quartier_equipement"),
                }
        print(f"  ✅ Capacités salles : {len(capacites)} équipements")
    else:
        print("  ⚠️  salles_capacites.json manquant — capacités non disponibles")

    return equipements, ppri, chaleur, lieux_frais, capacites

# ── Étape 2 : Filtrer les équipements pertinents ──────────────────────────────

def filter_refuges(equipements):
    print("\n[2/4] Filtrage des équipements refuge...")
    if equipements is None or equipements.empty:
        print("  ⚠️  Pas d'équipements — arrêt du traitement")
        return None

    # Détecter la colonne THEME (casse variable selon l'export)
    theme_col = next((c for c in equipements.columns if c.upper() == "THEME"), None)
    nom_col   = next((c for c in equipements.columns if c.upper() == "NOM"), None)

    if theme_col:
        refuges = equipements[equipements[theme_col].isin(THEMES_CODES)].copy()
        if len(refuges) == 0:
            # Fallback : garder tout si aucun code reconnu
            print("  ⚠️  Aucun thème reconnu — garde tous les équipements")
            refuges = equipements.copy()
    else:
        print("  ⚠️  Colonne THEME non trouvée — on garde tous les équipements")
        refuges = equipements.copy()

    print(f"  ℹ️  {len(refuges)} équipements après filtrage thématique")
    return refuges

# ── Étape 3 : Stress-test risques ────────────────────────────────────────────

def stress_test(refuges, ppri, chaleur):
    print("\n[3/4] Stress-test des risques...")

    refuges = normalize_crs(refuges)

    # --- Risque inondation ---
    refuges["flood_risk"] = "none"
    refuges["flood_niveau"] = None

    if ppri is not None and not ppri.empty:
        ppri = normalize_crs(ppri)

        # Filtrer uniquement les zones réellement inondables (soumisalea=OUI)
        if "soumisalea" in ppri.columns:
            ppri = ppri[ppri["soumisalea"].str.upper() == "OUI"].copy()
            print(f"  ℹ️  PPRI : {len(ppri)} zones inondables actives (soumisalea=OUI)")

        # Renommer les colonnes ambiguës avant la jointure pour éviter les conflits
        cols_a_renommer = {c: f"ppri_{c}" for c in ppri.columns
                          if c in ["nom", "id", "type", "codezone"] and c != "geometry"}
        ppri = ppri.rename(columns=cols_a_renommer)

        # Détecter colonne niveau d'aléa
        alea_col = next(
            (c for c in ppri.columns if any(k in c.lower() for k in ["codezone", "niveau", "alea", "zone"])),
            None
        )

        # Clip sur bbox Bordeaux pour accélérer
        from shapely.geometry import box as sbox
        bbox_bx = sbox(-0.65, 44.80, -0.52, 44.90)
        ppri_bx = ppri[ppri.intersects(bbox_bx)]

        # Buffer 50m autour de chaque refuge pour capturer les bâtiments
        # proches d'une zone inondable mais dont le centroïde est hors polygone
        refuges_proj = refuges[["geometry"]].to_crs(epsg=2154)  # Lambert 93 pour buffer en mètres
        refuges_buf  = refuges_proj.copy()
        refuges_buf["geometry"] = refuges_proj.buffer(50).to_crs(epsg=4326)

        join_flood = gpd.sjoin(refuges_buf, ppri_bx, how="left", predicate="intersects")
        refuges.loc[join_flood.index[~join_flood.index_right.isna()], "flood_risk"] = "high"

        # Stocker le codezone pour affiner la pénalité par horizon
        # Bleu clair = aléa faible (sera touché en 2040+)
        # Bleu = aléa modéré (touché maintenant)
        # Rouge/Grenat = aléa fort
        CODEZONE_SEVERITY = {
            "bleu clair":               "low",
            "bleu":                     "medium",
            "jaune":                    "medium",
            "orange":                   "medium",
            "rouge clair":              "high",
            "rouge":                    "high",
            "rouge urbanisé":           "high",
            "rouge non urbanisé":       "high",
            "rouge centre urbain":      "high",
            "rouge industrialo-portuaire": "high",
            "rouge hachurée jaune":     "high",
            "rouge foncé":              "high",
            "grenat":                   "high",
            "byzantin":                 "high",
        }

        if alea_col:
            for idx in join_flood.index[~join_flood.index_right.isna()]:
                ppri_idx = join_flood.loc[idx, "index_right"]
                if isinstance(ppri_idx, (int, float)) and int(ppri_idx) < len(ppri_bx):
                    codezone = str(ppri_bx.iloc[int(ppri_idx)].get(alea_col, "")).strip()
                    refuges.at[idx, "flood_niveau"] = codezone
                    # Classer la sévérité
                    severity = CODEZONE_SEVERITY.get(codezone.lower(), "medium")
                    refuges.at[idx, "flood_severity"] = severity
                    # Les zones "low" (bleu clair) ne sont pas encore inondables aujourd'hui
                    # mais le seront à partir de 2040 — on les marque différemment
                    if severity == "low":
                        refuges.at[idx, "flood_risk"] = "future"

        # Réinitialiser flood_severity pour les non-touchés
        if "flood_severity" not in refuges.columns:
            refuges["flood_severity"] = "none"
        refuges["flood_severity"] = refuges["flood_severity"].fillna("none")

        nb_flood  = (refuges["flood_risk"] == "high").sum()
        nb_future = (refuges["flood_risk"] == "future").sum()
        print(f"  ℹ️  Inondation : {nb_flood} équipements en zone à risque actuel, {nb_future} en zone à risque futur (bleu clair)")
    else:
        print("  ⚠️  PPRI non disponible — risque inondation non calculé")

    # --- Risque chaleur ---
    # Le dataset ri_icu_ifu_s utilise le champ "delta" :
    #   delta > 0  = îlot de chaleur (écart positif par rapport à la moyenne)
    #   delta < 0  = îlot de fraîcheur (parcs, eau — refuge naturel)
    # Seuils retenus :
    #   delta >= 6  → heat_risk = "high"   (très chaud, +6°C et plus)
    #   delta >= 2  → heat_risk = "moderate" (+2°C à +5°C)
    #   delta >= 0  → heat_risk = "low"    (légèrement au-dessus)
    #   delta < 0   → heat_risk = "cool"   (îlot de fraîcheur = bonus)
    refuges["heat_risk"] = "none"
    refuges["heat_intensite"] = 0.0

    if chaleur is not None and not chaleur.empty:
        chaleur = normalize_crs(chaleur)

        # Utiliser "delta" si disponible, sinon fallback sur ancienne logique
        delta_col = "delta" if "delta" in chaleur.columns else None

        join_heat = gpd.sjoin(refuges[["geometry"]], chaleur, how="left", predicate="intersects")
        touches_heat = join_heat.index[~join_heat.index_right.isna()]

        if delta_col:
            # Plusieurs polygones peuvent toucher un même refuge
            # On prend le delta MAX (pire cas) pour chaque refuge
            join_with_delta = join_heat.copy()
            import math
            def get_delta(i):
                try:
                    if i is None or (isinstance(i, float) and math.isnan(i)):
                        return None
                    idx = int(i)
                    if idx < len(chaleur):
                        return float(chaleur.iloc[idx]["delta"])
                except (ValueError, TypeError):
                    pass
                return None
            join_with_delta["delta_val"] = join_with_delta["index_right"].apply(get_delta)
            # Grouper par refuge, prendre le delta max
            max_delta = join_with_delta.groupby(join_with_delta.index)["delta_val"].max()

            for idx, delta in max_delta.items():
                if delta is None or (hasattr(delta, "__class__") and delta.__class__.__name__ == "float" and delta != delta):
                    continue
                try:
                    delta = float(delta)
                    # Filtrer les deltas extrêmement négatifs (Garonne, lacs)
                    # Un îlot de fraîcheur urbain a delta entre 0 et -5°C max
                    # En dessous de -5°C c'est un cours d'eau dans le dataset
                    if delta < -5:
                        delta = 0.0
                    refuges.at[idx, "heat_intensite"] = delta
                    if delta >= 6:
                        refuges.at[idx, "heat_risk"] = "high"
                    elif delta >= 2:
                        refuges.at[idx, "heat_risk"] = "moderate"
                    elif delta >= 0:
                        refuges.at[idx, "heat_risk"] = "low"
                    else:
                        # delta entre -5 et 0 = vrai îlot de fraîcheur urbain
                        refuges.at[idx, "heat_risk"] = "cool"
                except (ValueError, TypeError):
                    refuges.at[idx, "heat_risk"] = "moderate"
        else:
            for idx in touches_heat:
                refuges.at[idx, "heat_risk"] = "moderate"

        nb_high    = (refuges["heat_risk"] == "high").sum()
        nb_moderate= (refuges["heat_risk"] == "moderate").sum()
        nb_cool    = (refuges["heat_risk"] == "cool").sum()
        print(f"  ℹ️  Chaleur : {nb_high} îlots forts, {nb_moderate} modérés, {nb_cool} îlots de fraîcheur")
    else:
        print("  ⚠️  Îlots de chaleur non disponibles — risque chaleur non calculé")

    return refuges

# ── Étape 4 : Calcul du score de pérennité ────────────────────────────────────

def compute_score(flood_risk, heat_risk, heat_intensite, horizon):
    """
    Score de 0 (critique) à 100 (sûr).
    Pénalités de base aggravées par les coefficients horizon GIEC AR6 :
      - Inondation haute (rouge/grenat) : -45 pts dès maintenant
      - Inondation future (bleu clair)  : pénalité progressive à partir de 2040
      - Chaleur haute    : -20 pts × coeff chaleur
      - Chaleur modérée  : -15 pts × coeff chaleur
    """
    weights = HORIZONS.get(horizon, HORIZONS[2026])
    score = 100.0
    base_warming = {2026: 0, 2030: 2, 2040: 5, 2050: 10}
    score -= base_warming.get(horizon, 0)

    if flood_risk == "high":
        # Zone inondable dès maintenant — pénalité pleine
        score -= 45 * weights["flood_weight"]
    elif flood_risk == "future":
        # Zone bleu clair — devient inondable progressivement
        # Pas de pénalité en 2026/2030, pénalité progressive en 2040/2050
        future_penalties = {2026: 0, 2030: 5, 2040: 20, 2050: 35}
        score -= future_penalties.get(horizon, 0) * weights["flood_weight"]

    # Pénalité chaleur basée sur le delta réel (°C au-dessus de la moyenne)
    # Chaque degré de delta compte : pénalité = delta × 2.5 pts × coeff_horizon
    # Cela différencie un refuge à delta=2 (pénalité -5) d'un delta=5 (pénalité -12.5)
    if heat_intensite is not None:
        try:
            delta = float(heat_intensite)
        except (ValueError, TypeError):
            delta = 0.0
    else:
        # Fallback sur les catégories si pas de delta
        delta = {"high": 7.0, "moderate": 3.5, "low": 0.5, "cool": -2.0}.get(heat_risk, 0.0)

    if delta > 0:
        # Pénalité proportionnelle au delta : 2.5 pts par degré × coeff_chaleur
        score -= delta * 4.0 * weights["heat_weight"]
    elif delta < 0:
        # Bonus fraîcheur : 1 pt par degré de fraîcheur (plafonné à +5)
        bonus_decay = {2026: 1.0, 2030: 0.85, 2040: 0.65, 2050: 0.45}
        score += min(abs(delta) * 1.5, 5) * bonus_decay.get(horizon, 1.0)

    return max(0, min(100, round(score)))

def score_to_status(score):
    if score >= 75:
        return "sur"
    elif score >= 60:
        return "menace"
    else:
        return "critique"

# ── Étape 5 : Export JSON ─────────────────────────────────────────────────────

def export_json(refuges, capacites, ppri_ref=None, chaleur_ref=None):
    print("\n[4/4] Export JSON final...")

    nom_col   = next((c for c in refuges.columns if c.lower() in ["nom", "name", "acronyme"]), "nom")
    adr_col   = next((c for c in refuges.columns if c.lower() in ["adresse", "adrpost", "adr", "adresse_complete"]), None)
    theme_col = next((c for c in refuges.columns if c.lower() == "theme"), None)
    com_col   = next((c for c in refuges.columns if c.lower() in ["commune", "insee", "nom_com"]), None)

    result = []

    for idx, row in refuges.iterrows():
        try:
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue

            # Coordonnées (centroïde si polygone)
            point = geom if geom.geom_type == "Point" else geom.centroid
            lat = round(point.y, 6)
            lng = round(point.x, 6)

            nom    = str(row.get(nom_col, f"Équipement {idx}"))
            theme  = str(row.get(theme_col, "")) if theme_col else ""
            adresse = str(row.get(adr_col, "")) if adr_col else ""
            commune = str(row.get(com_col, "Bordeaux")) if com_col else "Bordeaux"

            # Récupérer capacité depuis dataset salles
            cap_info = capacites.get(nom.lower().strip(), {})
            superficie = cap_info.get("superficie")
            capacite   = cap_info.get("capacite")
            quartier   = cap_info.get("quartier", row.get("QUARTIER", ""))

            # Scores par horizon
            flood_risk     = str(row.get("flood_risk", "none"))
            heat_risk      = str(row.get("heat_risk", "none"))
            heat_intensite = float(row.get("heat_intensite", 0) or 0)

            scores = {
                str(h): {
                    "score": compute_score(flood_risk, heat_risk, heat_intensite, h),
                    "status": score_to_status(compute_score(flood_risk, heat_risk, heat_intensite, h))
                }
                for h in HORIZONS.keys()
            }

            # Recommandation automatique
            score_2026 = scores["2026"]["score"]
            if flood_risk == "high":
                reco = "Cet équipement est en zone inondable — il ne peut pas servir de refuge en cas de crue."
            elif heat_risk == "high":
                reco = "Situé en îlot de chaleur intense — prévoir climatisation ou ventilation renforcée."
            elif score_2026 >= 70:
                reco = "Refuge fiable. Capacité d'accueil suffisante, hors zone à risque majeur."
            else:
                reco = "Risque modéré. Vérifier l'accessibilité en cas d'événement climatique extrême."

            result.append({
                "id":    idx,
                "nom":   nom,
                "type":  theme,
                "adresse": adresse,
                "commune": commune,
                "quartier": quartier,
                "lat":   lat,
                "lng":   lng,
                "superficie": superficie,
                "capacite":   capacite,
                "care_officiel": is_care_officiel(nom),
                "risks": {
                    "flood":          flood_risk,
                    "flood_niveau":   str(row.get("flood_niveau", "")) or None,
                    "heat":           heat_risk,
                    "heat_intensite": heat_intensite,
                },
                "scores":    scores,
                "score":     score_2026,
                "status":    scores["2026"]["status"],
                "reco":      reco,
            })

        except Exception as e:
            print(f"  ⚠️  Erreur ligne {idx} : {e}")
            continue

    # Ajouter les CARE officiels manuels avec leurs scores calculés
    for care in CARE_MANUELS:
        # Calculer les risques via jointure spatiale plutôt qu'en dur
        from shapely.geometry import Point as SPoint
        import math

        pt = SPoint(care["lng"], care["lat"])

        # Risque inondation
        flood = "none"
        flood_niveau = None
        flood_severity = "none"
        if ppri_ref is not None and not ppri_ref.empty:
            ppri_ref_wgs = ppri_ref.to_crs(epsg=4326) if ppri_ref.crs.to_epsg() != 4326 else ppri_ref
            hits = ppri_ref_wgs[ppri_ref_wgs.contains(pt)]
            if len(hits) > 0:
                codezone = str(hits.iloc[0].get("ppri_codezone", hits.iloc[0].get("codezone", "")))
                flood_niveau = codezone
                SEVERITY = {
                    "bleu clair": "low", "bleu": "medium", "jaune": "medium",
                    "rouge clair": "high", "rouge": "high", "rouge urbanisé": "high",
                    "rouge non urbanisé": "high", "rouge centre urbain": "high",
                    "grenat": "high", "byzantin": "high",
                }
                sev = SEVERITY.get(codezone.lower(), "medium")
                flood = "future" if sev == "low" else "high"
                flood_severity = sev

        # Risque chaleur
        heat = "none"
        heat_intensite = 0.0
        if chaleur_ref is not None and not chaleur_ref.empty:
            chaleur_wgs = chaleur_ref.to_crs(epsg=4326) if chaleur_ref.crs.to_epsg() != 4326 else chaleur_ref
            hits_h = chaleur_wgs[chaleur_wgs.contains(pt)]
            if len(hits_h) > 0 and "delta" in hits_h.columns:
                try:
                    delta = float(hits_h["delta"].max())
                    heat_intensite = delta
                    if delta >= 6:   heat = "high"
                    elif delta >= 2: heat = "moderate"
                    elif delta >= 0: heat = "low"
                    else:            heat = "cool"
                except (ValueError, TypeError):
                    heat = "moderate"

        scores_care = {
            str(h): {
                "score": compute_score(flood, heat, heat_intensite, h),
                "status": score_to_status(compute_score(flood, heat, heat_intensite, h))
            }
            for h in HORIZONS.keys()
        }
        sc = scores_care["2026"]["score"]
        if flood == "high":
            reco = "⚠️ CARE officiel en zone inondable — à réévaluer pour les crises futures."
        elif flood == "future":
            reco = "⚠️ CARE officiel en zone bleu clair — risque inondation croissant d'ici 2050."
        else:
            reco = "CARE officiel activé lors de la crise de février 2026."

        result.append({
            **{k: v for k, v in care.items() if k != "note"},
            "risks": {
                "flood": flood, "flood_niveau": flood_niveau,
                "heat": heat, "heat_intensite": heat_intensite,
            },
            "scores": scores_care, "score": sc,
            "status": score_to_status(sc), "reco": reco,
        })

    # Trier par score décroissant
    result.sort(key=lambda x: x["score"], reverse=True)

    # Stats
    total   = len(result)
    surs    = sum(1 for r in result if r["status"] == "sur")
    menaces = sum(1 for r in result if r["status"] == "menace")
    crit    = sum(1 for r in result if r["status"] == "critique")
    cares   = sum(1 for r in result if r["care_officiel"])

    meta = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "commune": "Bordeaux",
        "total_refuges": total,
        "stats": {
            "sur": surs,
            "menace": menaces,
            "critique": crit,
            "care_officiels": cares,
        },
        "methodologie": (
            "Score calculé en croisant équipements Sport-loisir/Santé/Sénior "
            "avec PPRI (inondation) et îlots de chaleur. "
            "Pondérations aggravées par horizon temporel selon tendances IPCC."
        )
    }

    output = {"meta": meta, "refuges": result}
    out_path = PROC_DIR / "refuges_diagnostiques.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  ✅ {total} refuges exportés → {out_path}")
    print(f"     Sûrs : {surs} | Menacés : {menaces} | Critiques : {crit}")
    print(f"     CARE officiels identifiés : {cares}")

    return result, meta

# ── Parcs et jardins ─────────────────────────────────────────────────────────

def inject_parcs(result, ppri, chaleur):
    """
    Charge les parcs/jardins de Bordeaux et les injecte dans le JSON final
    comme refuges de fraîcheur (type='parc').
    Les parcs ont une capacité illimitée en plein air mais sont inutilisables
    en cas de pluie/inondation.
    """
    parcs_path = RAW_DIR / "parcs_jardins.geojson"
    if not parcs_path.exists():
        print("  ⚠️  parcs_jardins.geojson non trouvé — parcs non intégrés")
        return result

    parcs_gdf = gpd.read_file(str(parcs_path)).to_crs(epsg=4326)
    print(f"  ✅ Parcs : {len(parcs_gdf)} espaces verts chargés")

    # Préparer PPRI et chaleur
    ppri_ok = ppri is not None and not ppri.empty
    chaleur_ok = chaleur is not None and not chaleur.empty
    if ppri_ok:
        ppri_wgs = ppri.to_crs(epsg=4326) if ppri.crs.to_epsg() != 4326 else ppri
        from shapely.geometry import box as sbox
        bbox_bx = sbox(-0.65, 44.80, -0.52, 44.90)
        ppri_bx = ppri_wgs[ppri_wgs.intersects(bbox_bx)]

    parcs_added = 0
    for idx, row in parcs_gdf.iterrows():
        try:
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            centroid = geom.centroid
            lat = round(centroid.y, 6)
            lng = round(centroid.x, 6)
            nom = str(row.get("nom", f"Parc {idx}"))
            typo = str(row.get("typologie", "Espace vert"))

            # Risque inondation
            flood = "none"
            flood_niveau = None
            if ppri_ok:
                hits = ppri_bx[ppri_bx.intersects(geom)]
                if len(hits) > 0:
                    codezone = str(hits.iloc[0].get("ppri_codezone",
                                  hits.iloc[0].get("codezone", "Bleu")))
                    flood_niveau = codezone
                    SEVERITY = {
                        "bleu clair": "low", "bleu": "medium",
                        "rouge": "high", "rouge urbanisé": "high",
                        "rouge non urbanisé": "high", "grenat": "high",
                    }
                    sev = SEVERITY.get(codezone.lower(), "medium")
                    flood = "future" if sev == "low" else "high"

            # Risque chaleur — les parcs sont souvent des îlots de fraîcheur
            heat = "none"
            heat_intensite = 0.0
            if chaleur_ok:
                chaleur_wgs = chaleur.to_crs(epsg=4326) if chaleur.crs.to_epsg() != 4326 else chaleur
                hits_h = chaleur_wgs[chaleur_wgs.intersects(geom)]
                if len(hits_h) > 0 and "delta" in hits_h.columns:
                    delta = float(hits_h["delta"].mean())  # moyenne sur le parc
                    if delta < -5:
                        delta = -3.0  # cap à -3 pour les lacs/rivières
                    heat_intensite = round(delta, 1)
                    if delta >= 6:   heat = "high"
                    elif delta >= 2: heat = "moderate"
                    elif delta >= 0: heat = "low"
                    else:            heat = "cool"
                else:
                    # Fallback : delta moyen de Bordeaux si pas de données ICU
                    # Source : dataset ri_icu_ifu_s, moyenne = 3.5°C
                    heat_intensite = 3.5
                    heat = "moderate"

            scores = {
                str(h): {
                    "score": compute_score(flood, heat, heat_intensite, h),
                    "status": score_to_status(compute_score(flood, heat, heat_intensite, h))
                }
                for h in HORIZONS.keys()
            }
            sc = scores["2026"]["score"]

            # Exclure les parcs en zone inondable — inaccessibles en crise
            if flood != "none":
                continue

            if heat == "cool":
                reco = "Îlot de fraîcheur naturel — refuge thermique recommandé en canicule."
            elif heat in ["low", "none"]:
                reco = "Espace vert urbain — refuge thermique de jour en vague de chaleur."
            else:
                reco = "Espace vert en îlot de chaleur — fraîcheur limitée en canicule."

            result.append({
                "id": 8000 + idx,
                "nom": nom,
                "type": "parc",
                "adresse": typo,
                "commune": "Bordeaux",
                "quartier": "",
                "lat": lat,
                "lng": lng,
                "superficie": None,
                "capacite": None,
                "care_officiel": False,
                "risks": {
                    "flood": flood,
                    "flood_niveau": flood_niveau,
                    "heat": heat,
                    "heat_intensite": heat_intensite,
                },
                "scores": scores,
                "score": sc,
                "status": score_to_status(sc),
                "reco": reco,
            })
            parcs_added += 1
        except Exception as e:
            continue

    print(f"  ✅ {parcs_added} parcs ajoutés comme refuges de fraîcheur")
    return result

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  BordeauxSafePlace — Stress-test des données")
    print("=" * 55)

    equipements, ppri, chaleur, lieux_frais, capacites = load_all()
    refuges = filter_refuges(equipements)

    if refuges is not None and not refuges.empty:
        refuges = stress_test(refuges, ppri, chaleur)
        result, meta = export_json(refuges, capacites, ppri_ref=ppri, chaleur_ref=chaleur)

        # Injecter les parcs et jardins
        print("\n[+] Injection des parcs et jardins...")
        result = inject_parcs(result, ppri, chaleur)

        # Re-trier et re-sauvegarder
        result.sort(key=lambda x: x["score"], reverse=True)
        meta["total_refuges"] = len(result)
        meta["stats"]["sur"]     = sum(1 for r in result if r["status"] == "sur")
        meta["stats"]["menace"]  = sum(1 for r in result if r["status"] == "menace")
        meta["stats"]["critique"]= sum(1 for r in result if r["status"] == "critique")

        out_path = PROC_DIR / "refuges_diagnostiques.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"meta": meta, "refuges": result}, f, ensure_ascii=False, indent=2)
        print(f"  ✅ JSON final : {len(result)} refuges (équipements + parcs)")
        print(f"     Sûrs : {meta['stats']['sur']} | Menacés : {meta['stats']['menace']} | Critiques : {meta['stats']['critique']}")
        print("\n  Étape suivante : ouvrir frontend/bordeaux_safe_place.html")
    else:
        print("\n  ❌ Aucun refuge à traiter — vérifiez fetch_data.py")

    print("=" * 55)