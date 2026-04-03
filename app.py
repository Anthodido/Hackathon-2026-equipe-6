"""
app.py — BordeauxSafePlace Backend
====================================
Lance l'application avec : python app.py
Accessible sur : http://localhost:5000
"""

from flask import Flask, send_from_directory, jsonify, abort
from pathlib import Path
import json
import os

app = Flask(__name__)

BASE_DIR = Path(__file__).parent
FRONTEND_DIR = BASE_DIR / "frontend"
DATA_DIR = BASE_DIR / "data"

# ── Pages HTML ────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")

@app.route("/<path:filename>")
def static_files(filename):
    """Sert tous les fichiers statiques du dossier frontend."""
    if (FRONTEND_DIR / filename).exists():
        return send_from_directory(FRONTEND_DIR, filename)
    abort(404)

# ── API Données ───────────────────────────────────────────────

@app.route("/api/refuges")
def get_refuges():
    """Retourne tous les refuges avec leurs scores."""
    path = DATA_DIR / "processed" / "refuges_diagnostiques.json"
    if not path.exists():
        abort(404, "Fichier refuges_diagnostiques.json non trouvé — lancez stress_test.py")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return jsonify(data)

@app.route("/api/refuges/<int:refuge_id>")
def get_refuge(refuge_id):
    """Retourne un refuge spécifique par ID."""
    path = DATA_DIR / "processed" / "refuges_diagnostiques.json"
    if not path.exists():
        abort(404)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    refuge = next((r for r in data.get("refuges", []) if r["id"] == refuge_id), None)
    if not refuge:
        abort(404, f"Refuge {refuge_id} non trouvé")
    return jsonify(refuge)

@app.route("/api/stats")
def get_stats():
    """Retourne les statistiques globales par horizon."""
    path = DATA_DIR / "processed" / "refuges_diagnostiques.json"
    if not path.exists():
        abort(404)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    refuges = data.get("refuges", [])
    stats = {}
    for h in ["2026", "2030", "2040", "2050"]:
        stats[h] = {
            "sur":      sum(1 for r in refuges if r["scores"][h]["status"] == "sur"),
            "menace":   sum(1 for r in refuges if r["scores"][h]["status"] == "menace"),
            "critique": sum(1 for r in refuges if r["scores"][h]["status"] == "critique"),
        }

    return jsonify({
        "total": len(refuges),
        "care_officiels": sum(1 for r in refuges if r.get("care_officiel")),
        "parcs": sum(1 for r in refuges if r.get("type") == "parc"),
        "equipements": sum(1 for r in refuges if not r.get("care_officiel") and r.get("type") != "parc"),
        "par_horizon": stats,
    })

@app.route("/api/catnat")
def get_catnat():
    """Retourne l'historique des catastrophes naturelles."""
    path = DATA_DIR / "raw" / "catnat_bordeaux.json"
    if not path.exists():
        abort(404)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return jsonify(data)

@app.route("/api/care")
def get_care():
    """Retourne la liste des CARE officiels."""
    path = DATA_DIR / "processed" / "care_all.json"
    if not path.exists():
        abort(404)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return jsonify(data)

@app.route("/api/communes")
def get_communes():
    """Retourne les polygones des communes."""
    path = DATA_DIR / "raw" / "communes_bm.geojson"
    if not path.exists():
        abort(404)
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return jsonify(data)

# ── Données GeoJSON (couches carte) ──────────────────────────

GEOJSON_FILES = {
    "zones_inondables":      DATA_DIR / "processed" / "zones_inondables_clipped.geojson",
    "ilots_chaleur":         DATA_DIR / "raw"       / "ilots_chaleur.geojson",
    "fontaines":             DATA_DIR / "raw"       / "fontaines.geojson",
    "hydrographie":          DATA_DIR / "raw"       / "hydrographie.geojson",
    "zone_inondable_2026":   DATA_DIR / "processed" / "zone_inondable_2026.geojson",
    "zone_inondable_2030":   DATA_DIR / "processed" / "zone_inondable_2030.geojson",
    "zone_inondable_2040":   DATA_DIR / "processed" / "zone_inondable_2040.geojson",
    "zone_inondable_2050":   DATA_DIR / "processed" / "zone_inondable_2050.geojson",
}

@app.route("/api/geo/<layer>")
def get_geojson(layer):
    """Retourne une couche GeoJSON par nom."""
    if layer not in GEOJSON_FILES:
        abort(404, f"Couche '{layer}' inconnue. Disponibles : {list(GEOJSON_FILES.keys())}")
    path = GEOJSON_FILES[layer]
    if not path.exists():
        abort(404, f"Fichier {path.name} non trouvé")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return jsonify(data)

@app.route("/api/geo")
def list_geojson():
    """Liste les couches GeoJSON disponibles."""
    available = {k: v.exists() for k, v in GEOJSON_FILES.items()}
    return jsonify(available)

# ── Santé ─────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    files = {
        "refuges":      (DATA_DIR / "processed" / "refuges_diagnostiques.json").exists(),
        "ppri":         (DATA_DIR / "processed" / "zones_inondables_clipped.geojson").exists(),
        "chaleur":      (DATA_DIR / "raw"       / "ilots_chaleur.geojson").exists(),
        "catnat":       (DATA_DIR / "raw"       / "catnat_bordeaux.json").exists(),
        "care":         (DATA_DIR / "processed" / "care_all.json").exists(),
        "fontaines":    (DATA_DIR / "raw"       / "fontaines.geojson").exists(),
        "communes":     (DATA_DIR / "raw"       / "communes_bm.geojson").exists(),
        "hydro_2050":   (DATA_DIR / "processed" / "zone_inondable_2050.geojson").exists(),
    }
    all_ok = all(files.values())
    return jsonify({
        "status": "ok" if all_ok else "partial",
        "fichiers": files,
        "manquants": [k for k, v in files.items() if not v]
    }), 200 if all_ok else 206

# ── Lancement ─────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  BordeauxSafePlace — Backend")
    print("=" * 55)
    print(f"  Frontend : {FRONTEND_DIR}")
    print(f"  Données  : {DATA_DIR}")
    print()
    print("  Endpoints disponibles :")
    print("    GET /                    → index.html")
    print("    GET /map.html            → carte interactive")
    print("    GET /api/refuges         → tous les refuges")
    print("    GET /api/refuges/<id>    → un refuge")
    print("    GET /api/stats           → stats par horizon")
    print("    GET /api/catnat          → historique inondations")
    print("    GET /api/care            → CARE officiels")
    print("    GET /api/communes        → polygones communes")
    print("    GET /api/geo/<layer>     → couche GeoJSON")
    print("    GET /api/health          → état des données")
    print()
    print("  → http://localhost:5000")
    print("=" * 55)

    app.run(debug=True, host="0.0.0.0", port=5000)
