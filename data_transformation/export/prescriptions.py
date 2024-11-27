from typing import Dict
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import requests
import time
import configparser
from data_transformation.data_transfer.utils import Connection, DataTransfer


def getRxNormIdByNDC(ndc: int):
    ndc = '0' * (11 - len(str(ndc))) + str(ndc)
    url = f'https://rxnav.nlm.nih.gov/REST/rxcui?idtype=NDC&id={ndc}'
    header = {'Accept': 'application/json'}
    result = requests.get(url, headers=header).json()['idGroup']
    return int(result['rxnormId'][0]) if 'rxnormId' in result else None


def getDrugClass(rxnormId: int):
    if not rxnormId:
        return 'Not Classified'
    url = f'https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxnormId}'
    header = {'Accept': 'application/json'}
    result = requests.get(url, headers=header).json()['rxclassDrugInfoList']['rxclassDrugInfo']
    classes = []
    for entry in result:
        if entry['rxclassMinConceptItem']['classType'] in ['MOA', 'PE', 'TC', 'EPC']:
                # and entry['minConcept']['rxcui'] == rxnormId:
            classes.append(entry['rxclassMinConceptItem']['className'])
    return classes if classes else ['Not Classified']


def process_prescription(index_and_prescription):
    try:
        i, prescription = index_and_prescription
        ndc = int(prescription['ndc'])
        rxnorm = getRxNormIdByNDC(ndc)
        drug_class = getDrugClass(rxnorm)
        prescription.update({'drug_class': drug_class})
        print(i, prescription['drug'], int(prescription['ndc']), rxnorm, drug_class)
        return prescription
    except Exception as e:
        print(f"Error processing prescription {prescription}: {e}")
        return prescription


config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']

dt = DataTransfer(Connection(*(x[1] for x in config.items('database'))))

prescriptions = dt.fetch_data(
    """
    SELECT DISTINCT ndc
        , LOWER(TRIM(drug)) AS drug
    FROM raw.prescriptions
    WHERE ndc IS NOT NULL AND ndc > 0
    """
).to_dict('records')

indexed_prescriptions = list(enumerate(prescriptions))
start_time = time.time()

with ThreadPoolExecutor(max_workers=10) as executor:
    updated_prescriptions = list(executor.map(process_prescription, indexed_prescriptions))

end_time = time.time()

print(f"Processed {len(prescriptions)} prescriptions in {end_time - start_time:.2f} seconds")

pd.DataFrame(updated_prescriptions).to_excel("drug_classes.xlsx")
