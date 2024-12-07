"""Module to create an export file of prescriptions"""

from concurrent.futures import ThreadPoolExecutor
import time
import configparser
from typing import Union, List, Dict, Tuple

import pandas as pd
import requests
from data_transformation.data_transfer.utils import ConnectionDetails, DataTransfer


def get_rxnormid_by_ndc(ndc: int) -> Union[int, None]:
    """
    :param ndc: NDC of a drug
    :return: rxNormId
    """

    ndc = '0' * (11 - len(str(ndc))) + str(ndc)
    url = f'https://rxnav.nlm.nih.gov/REST/rxcui?idtype=NDC&id={ndc}'
    header = {'Accept': 'application/json'}
    result = requests.get(url, headers=header, timeout=10).json()['idGroup']
    return int(result['rxnormId'][0]) if 'rxnormId' in result else None


def get_drug_class(rxnormid: int) -> List[str]:
    """
    :param rxnormid: RxNormId of a drug
    :return: list of drug classes
    """

    if not rxnormid:
        return ['Not Classified']
    url = f'https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui={rxnormid}'
    header = {'Accept': 'application/json'}
    result = requests.get(url, headers=header, timeout=10).json()['rxclassDrugInfoList']['rxclassDrugInfo']
    classes = []
    for entry in result:
        if entry['rxclassMinConceptItem']['classType'] in ['MOA', 'PE', 'TC', 'EPC']:
                # and entry['minConcept']['rxcui'] == rxnormid:
            classes.append(entry['rxclassMinConceptItem']['className'])
    return classes if classes else ['Not Classified']


def process_prescription(index_and_prescription: Tuple[int, dict]) -> Dict:
    """
    :param index_and_prescription: index and prescription
    :return: updated prescription with the drug class
    """
    try:
        i, prescription = index_and_prescription
        ndc = int(prescription['ndc'])
        rxnorm = get_rxnormid_by_ndc(ndc)
        drug_class = get_drug_class(rxnorm)
        prescription.update({'drug_class': drug_class})
        print(i, prescription['drug'], int(prescription['ndc']), rxnorm, drug_class)
        return prescription
    except Exception as e:
        print(f"Error processing prescription {prescription}: {e}")
        return prescription

# pylint: disable=duplicate-code
config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']
# pylint: enable=duplicate-code

dt = DataTransfer(ConnectionDetails(*(x[1] for x in config.items('database'))))

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
