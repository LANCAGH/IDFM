# Projet IDFM
## Hugo LANCA

**Ce README.md est susceptible d'évoluer à mesure que je complète le projet. Voici un résumé complet des différentes prises de décision.**
---

## Contexte métier

**Besoin fictif** : Île-de-France Mobilités souhaite prioriser ses investissements en accessibilité PMR sur le réseau ferré (RER, métro, Transilien). L'enjeu est d'identifier quelles stations doivent être rénovées en premier, en croisant leur fréquentation réelle avec leur niveau d'accessibilité actuel.

**Périmètre** : Réseau ferré uniquement (hors bus et tram), données 2024 uniquement. Simplification volontaire du périmètre initial (2015-2024) pour se concentrer sur l'année la plus récente disponible. Les 3 fichiers 2024 (S1, T3, T4) sont concaténés pour former une année complète.

---

## 1. Architecture Medallion (Bronze / Silver / Gold)

**Décision** : Pipeline organisé en 3 couches de données sur AWS S3.
![alt text](<Capture d’écran 2026-03-15 à 13.39.50.png>)

**Justification** : D'après AWS Academy (Data Engineering) séparer la donnée brute (Bronze) de la donnée nettoyée (Silver) et métier (Gold) permet de retracer l'origine de chaque transformation, de rejouer une étape sans tout recalculer, et d'assurer la traçabilité.

Structure du projet : 
```
s3://p-idfm-pipeline/
├── bronze/          ← données brutes, immuables
├── silver/          ← données nettoyées, typées, jointes
├── gold/            ← score de priorité PMR, prêt pour le dashboard
├── profiling/       ← rapports d'exploration de la donnée
├── quality_reports/ ← résultats des tests qualité à chaque run
├── athena_results/  ← résultats athena
└── pipeline_metadata/ ← checkpoints et métadonnées de run
```
---

## 1bis. IAM — Principe du moindre privilège

**Conformité** : Les rôles IAM créés respectent le principe de `least privilege`. Chaque service dispose uniquement des permissions strictement nécessaires à son fonctionnement.

| Rôle | Service | Policies attachées |
|---|---|---|
| `RoleLambda-idfmpipeline` | AWS Lambda | `p-idfm-lambda-s3-write`, `p-idfm-pipeline-jobglue` |
| `GlueRole-idfmpipeline` | AWS Glue | `AWSGlueServiceRole`, `GetWriteObject-idfmpipeline` |
| `hugo-idfm-dev` (utilisateur IAM) | AWS CLI / boto3 local | `GetAthena-idfmpipeline`, `Get-Glue-idfmpipeline`, `GetWriteObject-idfmpipeline` |

J'ai utilisé les sections `roles`et `policies` de AWS IAM afin de créer des policies et les affecter aux rôles.
---

## 2. Stack technique — AWS natif uniquement

**Décision** : Lambda (ingestion) + Glue Python Shell (transformation) + S3 (stockage) + Athena (requêtage) + EventBridge (orchestration) + Power BI Desktop (visualisation).

**Justification** : Je me suis lancé le défi de réaliser le projet avec un budget de 0€ (Free Tier) : une stack AWS native permet de livrer un pipeline complet sans disperser l'effort. **dbt** aurait apporté de la gouvernance supplémentaire mais allongeait significativement le délai. Il est mentionné dans le README comme évolution naturelle. Dans le meilleur scénario, j'aurais dû configurer AWS avec du code `IAC`. J'ai installé le `CLI`.

**Orchestration des jobs Glue** : Lambda déclenche les deux jobs Glue en séquence via `start_job_run` (boto3) — cleaning puis curation. Step Functions est identifié comme évolution v2 pour une orchestration plus robuste avec gestion des états et retry.

**Flux d'ingestion — ce qui a été réellement fait** :

L'ingestion historique (2015-2024) a été réalisée via un script Python exécuté en local (`ingestion_NB_FER_hist.py`), qui écrit directement dans S3 via `boto3` avec les credentials AWS CLI configurés localement. Les données sont présentes dans la couche Bronze.

Le code Lambda (`ingestion_NB_FER_hist.py`) a été rédigé et structuré avec le point d'entrée `lambda_handler(event, context)` pour les ingestions semestrielles futures, mais **n'a pas été déployé sur AWS Lambda** dans le cadre de ce projet. Le flux prévu est :
1. Appel API `/exports/json` → JSON contenant l'URL du fichier ZIP de l'année
2. Téléchargement du ZIP depuis `reseau_ferre.url`
3. Extraction du CSV depuis le ZIP, écriture en Bronze sur S3

**Environnements dev / prod** :

En production, la bonne pratique est de séparer les environnements avec deux buckets distincts (`p-idfm-pipeline-dev` et `p-idfm-pipeline-prod`) et deux fonctions Lambda séparées partageant le même code mais des variables d'environnement différentes. Cette séparation permet de tester une ingestion sur des données récentes sans risquer d'écraser les données de production.

Dans le cadre de ce projet, un seul bucket est utilisé (`p-idfm-pipeline`). L'ingestion historique ayant été exécutée une seule fois en local, le risque d'écrasement était maîtrisé. Cette décision est un compromis volontaire lié au périmètre portfolio du projet.

---

## 3. Granularité de l'analyse — Zone de Correspondance (ZdC)

IDFM distingue clairement les **ZdC** des **ZdA**. 
- ZdA (Zone d'Arrêt) = un quai, une entrée, un point physique précis.
- ZdC (Zone de Correspondance) = la station dans son ensemble, qui regroupe toutes ses ZdA.

![alt text](<Capture d’écran 2026-03-15 à 13.42.39.png>)

**Décision** : L'unité d'analyse est la ZdC (station multimodale), pas la ZdA (arrêt monomodal).

**Justification** : Sur le réseau ferré, la validation se fait à l'entrée de la station indépendamment de la ligne empruntée (source : documentation IDFM). Les validations sont donc physiquement impossibles à ventiler par ZdA — la ZdC est la granularité réelle des données.

**Jointure** :


```
validations.ID_REFA_LDA (ZdC)
    → référentiel Zones d'arrêts (table pivot ZdC ↔ ZdA)
    → score PMR = MIN(accessibilité de toutes les ZdA de la ZdC)
```

**Justification du MIN** : Une station est aussi peu accessible que son entrée la moins accessible. J'ai donc décidé que le MIN reflète l'expérience réelle d'un voyageur PMR.

---
## 4. Score de priorité PMR — Ranking

**Décision** : Pas de formule normalisée. Classement direct par :

1. Fréquentation totale décroissante
2. Score PMR croissant (1 = moins accessible = prioritaire)

En SQL : 
```sql
RANK() OVER (ORDER BY total_validations DESC, score_pmr ASC)
```

**Justification** : L'objectif métier est d'identifier les stations à fort trafic et faible accessibilité. Un classement est plus lisible et actionnable pour un décideur qu'un score composite normalisé. La comparabilité temporelle est assurée en comparant le podium d'une année à l'autre.

---

## 5. Normalisation des identifiants historiques

**Décision** : Hors scope — le périmètre étant limité à 2024, les anciens identifiants 2015-2016 ne sont pas traités. Les fichiers 2024 utilisent les identifiants courants `ID_REFA_LDA`.

---

## Évolutions futures (hors scope)

- **Step Functions** : remplacement de l'orchestration Lambda séquentielle pour une gestion des états, conditions et retry plus robuste
- **dbt** : remplacement de Glue pour les transformations Silver/Gold, apporte lineage, documentation et tests natifs
- **Infrastructure as Code** : AWS CDK pour recréer l'environnement from scratch en une commande
- **Tests unitaires** : pytest sur les fonctions Python de nettoyage et transformation
- **AWS Parameter Store** : si j'ai des clés avec des secrets (clés API privées)
