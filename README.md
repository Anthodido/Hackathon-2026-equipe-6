# Hackathon-2026-equipe-6

----

📊Diagram Mermaid

```mermaid
flowchart LR

  subgraph PROB["Défi territorial"]
    direction TB
    P1(["Bordeaux Métropole\n28 communes · 831 534 hab."])
    P2(["46% exposés\nà 3+ aléas climatiques"])
    P3(["Plan de résilience\nadopté juin 2025"])
    P1 --> P2 --> P3
  end

  subgraph SRC["Sources de données ouvertes"]
    direction TB
    DB1[("DataHub BM\n547 jeux")]
    DB2[("IPCC AR6 2021")]
    DB3[("DRIAS 2020\nMétéo-France")]
    DB4[("Géorisques\nBRGM · IGN")]
    DB5[("INSEE")]
  end

  subgraph ENG["Moteur d'analyse des risques"]
    direction TB
    E1["Score par aléa — 0 à 10\npar commune"]
    E2["Score composite\nmoyenne 5 aléas"]
    E3["Sensibilité IPCC\n+4% à +20% par °C"]
    E4["Projection linéaire\n2025 → 2100"]
    E1 --> E2 --> E3 --> E4
  end

  subgraph APP["RésilienCarte — Application web"]
    direction TB
    A1["Carte interactive\n28 communes · Leaflet.js"]
    A2["5 couches d'aléas\nfiltrage temps réel"]
    A3["Profil commune\nradar · barres · faits clés"]
    A4["Simulation 2025-2100\n3 scénarios IPCC · 5 KPIs"]
    A1 --- A2 --- A3 --- A4
  end

  subgraph SCEN["Scénarios IPCC AR6"]
    direction TB
    SC1{{"SSP1-2.6 Optimiste\n+1.3°C en 2100"}}
    SC2{{"SSP2-4.5 Modéré\n+3.0°C en 2100"}}
    SC3{{"SSP5-8.5 Pessimiste\n+5.6°C en 2100"}}
  end

  subgraph OUT["Impact & valeur ajoutée"]
    direction TB
    O1(["Aide à la décision\npour les élus"])
    O2(["Prévention ciblée\npar zone et risque"])
    O3(["Transparence citoyenne"])
  end

  PROB == contexte ==> SRC
  SRC == données ==> ENG
  ENG == calcul ==> APP
  SCEN -- intégrés dans --> A4
  APP == résultats ==> OUT
```
