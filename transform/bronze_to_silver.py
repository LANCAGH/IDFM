import yaml
import boto3
import pandas as pd
import pyarrow
import io

# - - - - Le code est extrait de la partie exploration du Notebook - - - - -

#CONFIG

s3_client = boto3.client('s3')

CONFIG_BUCKET = "p-idfm-pipeline"
CONFIG_KEY = "config/config.yaml"

obj = s3_client.get_object(Bucket=CONFIG_BUCKET, Key=CONFIG_KEY)
yaml_file = yaml.safe_load(io.BytesIO(obj['Body'].read()))

bucket = yaml_file['S3']['bucket']
bronze_prefix = yaml_file['S3']['chemins_dossiers']['bronze']
silver_prefix = yaml_file['S3']['chemins_dossiers']['silver']
filtre_zdatype = yaml_file['Pipeline']['filtre_ZdAtype']

#CHARGEMENT BRONZE

list_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=bronze_prefix)['Contents']

# NB_FER_2024 : concaténation S1 + T3 + T4
scope_objects = [i['Key'] for i in list_objects if '2024' in i['Key']]

dfs = []
for i in scope_objects:
    obj = s3_client.get_object(Bucket=bucket, Key=i)
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    dfs.append(df)

NB_FER_2024 = pd.concat(dfs, ignore_index=True)

# ACCESSIBILITE
scope_object_access = [i['Key'] for i in list_objects if i['Key'] == "bronze/accessibilite_en_gare.parquet"][0]
obj = s3_client.get_object(Bucket=bucket, Key=scope_object_access)
ACCESSIBILITE = pd.read_parquet(io.BytesIO(obj['Body'].read()))

# REFERENCES
scope_object_references = [i['Key'] for i in list_objects if i['Key'] == "bronze/references.parquet"][0]
obj = s3_client.get_object(Bucket=bucket, Key=scope_object_references)
REFERENCES = pd.read_parquet(io.BytesIO(obj['Body'].read()))

# ── NETTOYAGE ─────────────────────────────────────────────────────────────────

# NB_FER : typage explicite des colonnes object pour éviter les ambiguïtés PyArrow
NB_FER_2024['JOUR'] = NB_FER_2024['JOUR'].astype(str)
NB_FER_2024['CODE_STIF_RES'] = NB_FER_2024['CODE_STIF_RES'].astype(str)
NB_FER_2024['CODE_STIF_ARRET'] = NB_FER_2024['CODE_STIF_ARRET'].astype(str)
NB_FER_2024['CATEGORIE_TITRE'] = NB_FER_2024['CATEGORIE_TITRE'].astype(str)

# NB_FER : conversion NB_VALD object → float64, suppression des lignes sans ID_ZDC
NB_FER_2024['NB_VALD'] = pd.to_numeric(NB_FER_2024['NB_VALD'], errors='coerce')
NB_FER_2024 = NB_FER_2024.dropna(subset=['ID_ZDC'])
NB_FER_2024['ID_ZDC'] = NB_FER_2024['ID_ZDC'].astype('Int64')

# ACCESSIBILITE : extraction de l'identifiant numérique depuis stop_point_id
# ex: "stop_point:IDFM:monomodalStopPlace:43069" → "43069"
ACCESSIBILITE['stop_point_id'] = ACCESSIBILITE['stop_point_id'].str.split(':').str[-1]
ACCESSIBILITE['accessibility_level_name'] = ACCESSIBILITE['accessibility_level_name'].astype(str)
ACCESSIBILITE['accessibility_level_id'] = ACCESSIBILITE['accessibility_level_id'].astype('Int64')

# REFERENCES : filtre sur les types de ZdA pertinents (métro, rail, lift)
REFERENCES = REFERENCES[REFERENCES['zdatype'].isin(filtre_zdatype)]
REFERENCES['zdcid'] = pd.to_numeric(REFERENCES['zdcid']).astype('Int64')
REFERENCES['zdaid'] = REFERENCES['zdaid'].astype(str)
REFERENCES['zdaname'] = REFERENCES['zdaname'].astype(str)
REFERENCES['zdatown'] = REFERENCES['zdatown'].astype(str)
REFERENCES['zdatype'] = REFERENCES['zdatype'].astype(str)

# ── JOINTURES ─────────────────────────────────────────────────────────────────

# Étape 1 : NB_FER_2024 → REFERENCES sur ID_ZDC = zdcid
silver = NB_FER_2024.merge(
    REFERENCES[['zdaid', 'zdcid', 'zdaname', 'zdatown', 'zdatype']],
    left_on='ID_ZDC',
    right_on='zdcid',
    how='inner'
)

# Étape 2 : résultat → ACCESSIBILITE sur zdaid = stop_point_id
silver = silver.merge(
    ACCESSIBILITE[['stop_point_id', 'accessibility_level_id', 'accessibility_level_name']],
    left_on='zdaid',
    right_on='stop_point_id',
    how='inner'
)

# ── EXPORT SILVER ─────────────────────────────────────────────────────────────

buffer = io.BytesIO()
silver.to_parquet(buffer, index=False)
buffer.seek(0)

s3_client.put_object(
    Bucket=bucket,
    Key=f"{silver_prefix}nb_fer_accessibilite_2024.parquet",
    Body=buffer.getvalue()
)

print(f"Silver écrit : {silver.shape[0]} lignes, {silver.shape[1]} colonnes")
