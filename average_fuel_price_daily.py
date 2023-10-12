from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import zipfile
import xml.etree.ElementTree as ET
import os

from airflow.configuration import conf

# Définir la configuration du DAG
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'analyze_average_fuel_prices',
    default_args=default_args,
    schedule='@daily',  # Exécutez le DAG quotidiennement
    catchup=False,  # Ignorez les exécutions en retard
)

def download_zip_file():
    url = "https://donnees.roulez-eco.fr/opendata/jour"
    response = requests.get(url)
    with open("/tmp/data.zip", "wb") as file:
        file.write(response.content)

# Fonction pour extraire le contenu du fichier ZIP
def extract_zip_file():
    with zipfile.ZipFile("/tmp/data.zip", "r") as zip_ref:
        # Recherchez un fichier XML à l'intérieur du ZIP sans connaître son nom à l'avance
        for filename in zip_ref.namelist():
            if filename.lower().endswith('.xml'):
                zip_ref.extract(filename, '/tmp')
                os.rename(os.path.join('/tmp', filename), '/tmp/data.xml')
                break  # Sortez de la boucle après avoir trouvé le premier fichier XML

# Fonction pour analyser le fichier XML
def analyze_xml_file():
    tree = ET.parse("/tmp/data.xml")  # Utilisez le nom data.xml pour l'analyse
    root = tree.getroot()
    
    fuel_prices_by_id = {}
    
    for pdv in root.findall('.//pdv'):
        for price in pdv.findall('.//prix'):
            id_fuel = price.get('id')
            value = price.get('valeur')
            
            if id_fuel in fuel_prices_by_id:
                fuel_prices_by_id[id_fuel].append(float(value))
            else:
                fuel_prices_by_id[id_fuel] = [float(value)]
    
    # Calculez le prix moyen par ID de carburant
    average_prices_by_id = {}
    for id_fuel, price_list in fuel_prices_by_id.items():
        average_price = sum(price_list) / len(price_list)
        average_prices_by_id[id_fuel] = average_price

    print("Average fuel prices by ID:")
    for id_fuel, average_price in average_prices_by_id.items():
        print(f"Fuel ID: {id_fuel}, Average Price: {average_price}")

# Opérateurs pour chaque étape du processus

download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_zip_file,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_zip_file,
    dag=dag,
)

calculate_average_fuel_price_task = PythonOperator(
    task_id='calculate_average_fuel_price_task',
    python_callable=analyze_xml_file,
    dag=dag,
)

download_task >> extract_task >> calculate_average_fuel_price_task
