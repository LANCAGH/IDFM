import yaml
import boto3
import pandas as pd
import pyarrow
import io

# - - - - Le code est extrait de la partie exploration du Notebook - - - - -

# CONFIG

s3_client = boto3.client('s3')

CONFIG_BUCKET = "p-idfm-pipeline"
CONFIG_KEY = "config/config.yaml"

obj = s3_client.get_object(Bucket=CONFIG_BUCKET, Key=CONFIG_KEY)
yaml_file = yaml.safe_load(io.BytesIO(obj['Body'].read()))

bucket = yaml_file['S3']['bucket']
silver_prefix = yaml_file['S3']['chemins_dossiers']['silver']
gold_prefix = yaml_file['S3']['chemins_dossiers']['gold']

# CHARGEMENT SILVER

obj = s3_client.get_object(Bucket=bucket, Key=f"{silver_prefix}nb_fer_accessibilite_2024.parquet")
df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

# TOTAL VALIDATIONS 
# Dédupliquer sur ID_ZDC + JOUR + CATEGORIE_TITRE pour éviter le doublon ZdA
# NB_VALD est au niveau ZdC, répété N fois (une fois par ZdA) dans la Silver

df_validations = df.drop_duplicates(subset=['ID_ZDC', 'JOUR', 'CATEGORIE_TITRE'])
df_grouped = df_validations.groupby('ID_ZDC')['NB_VALD'].sum().reset_index()

# SCORE PMR 
# idxmin() retourne l'index de la ligne avec le score PMR minimum par ZdC
# .loc[] extrait cette ligne pour récupérer le label associé au MIN

idx_min_pmr = df.groupby('ID_ZDC')['accessibility_level_id'].idxmin()
df_pmr = df.loc[
    idx_min_pmr.tolist(),
    ['ID_ZDC', 'accessibility_level_id', 'accessibility_level_name', 'zdaname', 'zdatown', 'zdatype']
].reset_index(drop=True)

# CONSTRUCTION GOLD

gold = df_pmr.merge(df_grouped, on='ID_ZDC', how='left')

# Classement : score PMR croissant (1 = prioritaire), puis fréquentation décroissante
gold = gold.sort_values(
    by=['accessibility_level_id', 'NB_VALD'],
    ascending=[True, False]
).reset_index(drop=True)

gold['rank_priorite'] = gold.index + 1

# Renommage pour lisibilité
gold = gold.rename(columns={
    'accessibility_level_id': 'score_pmr',
    'accessibility_level_name': 'score_pmr_label',
    'NB_VALD': 'total_validations'
})

# EXPORT GOLD 

buffer = io.BytesIO()
gold.to_parquet(buffer, index=False)
buffer.seek(0)

s3_client.put_object(
    Bucket=bucket,
    Key=f"{gold_prefix}pmr_priority_ranking.parquet",
    Body=buffer.getvalue()
)

print(f"Gold écrit : {gold.shape[0]} lignes, {gold.shape[1]} colonnes")
