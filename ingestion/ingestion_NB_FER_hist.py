import yaml
import pandas as pd
import requests
import zipfile
import io
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context) : 

    # à partir du fichier config.yaml...
    file = open("config/config.yaml")
    yaml_file = yaml.safe_load(file)
    file.close()

    #... on charge l'URL de l'API IDFM, on enregistre pour plus tard le chemin bronze
    url = yaml_file['Lambda']['URL_API_NBFER'] #on stocke l'url de l'API
    bronze_path = yaml_file['S3']['chemins_dossiers']['bronze'] #chemin des fichiers en bronze
    bucket = yaml_file['S3']['bucket'] #chemin du fichier
    """
    PARTIE REQUETE : tester avec requête HTTP
    """

    #retourne status : 200
    r= requests.get(url)
    r.raise_for_status() 
    #on stocke le retour json
    api_json_nb_fer = r.json()

    #list comprehension pour chaque url de la liste de dictionnaires
    urls_zip = [i['reseau_ferre']['url'] for i in api_json_nb_fer if i['reseau_ferre']]    

    # boucle pour créer un fichier temporaire en RAM pour chaque zip
    for url_zip in urls_zip:
        response = requests.get(url_zip)
        response.raise_for_status()
        contenu_zip = response.content #en bytes
        zip_buffer = io.BytesIO(contenu_zip) #zone temporaire en RAM
        zips_files = zipfile.ZipFile(zip_buffer) #on obtient le fichier zip
        fichiers = zips_files.namelist() #on obtient la liste des fichiers dans le zip
        fichiers_nb_fer = [i for i in fichiers if "NB_FER.txt" in i] #on ne garde que les fichiers pertinents
        
        # on commence la boucle imbriquée pour convertir chaque fichier en parquet
        for txt_file in fichiers_nb_fer:
            raw_bytes = zips_files.open(txt_file).read()                 
            if raw_bytes[:2] == b'\xff\xfe':
                df = pd.read_csv(io.BytesIO(raw_bytes), delimiter="\t", encoding="UTF-16")
            else:
                try:
                    df = pd.read_csv(io.BytesIO(raw_bytes), delimiter="\t", encoding="UTF-8")
                except UnicodeDecodeError:
                    df = pd.read_csv(io.BytesIO(raw_bytes), delimiter="\t", encoding="latin-1")
            parquet_buffer = io.BytesIO() #zone temporaire en RAM
            df.to_parquet(parquet_buffer,compression="snappy") #on fait gaffe le buffer est à la fin du fichier, on utilise .getvalue() donc ok
            s3_key = bronze_path + txt_file.replace('.txt','.parquet') #chemin du fichier
            #ressemble à bronze/2017_S2_NB_FER.parquet
            s3_client.put_object(
                Bucket= bucket,
                Key = s3_key,
                Body = parquet_buffer.getvalue()
            )

    return{"statusCode": 200}

if __name__ == "__main__":
    lambda_handler({}, None)