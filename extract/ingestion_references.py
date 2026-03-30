import yaml
import pandas as pd
import requests
import io
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):

    # à partir du fichier config.yaml...
    file = open("config/config.yaml")
    yaml_file = yaml.safe_load(file)
    file.close()

    #... on charge l'URL de l'API IDFM, on enregistre pour plus tard le chemin bronze
    url = yaml_file['Lambda']['URL_API_REF'] #on stocke l'url de l'API
    bronze_path = yaml_file['S3']['chemins_dossiers']['bronze'] #chemin des fichiers en bronze
    bucket = yaml_file['S3']['bucket'] #chemin du fichier

    #retourne status : 200
    r = requests.get(url)
    r.raise_for_status()

    #conversion directe JSON → DataFrame
    df = pd.DataFrame(r.json())

    #conversion Parquet en mémoire
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, compression='snappy', index=False)

    #écriture dans S3
    s3_key = bronze_path + 'references.parquet'
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=parquet_buffer.getvalue()
    )

    return {"statusCode": 200}


if __name__ == "__main__":
    lambda_handler({}, None)
