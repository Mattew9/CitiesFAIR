import json
import requests


def retrieve_mqa_point(dataset_id):
    try:
        response = requests.get(f"https://data.europa.eu/api/mqa/cache/datasets/{dataset_id}")

        if response.status_code == 200:
            payload = json.loads(response.text)

            return payload["result"]["results"][0]["info"]["score"]
        else:
            return 0

    except requests.exceptions:
        raise


def find_europa_id(dataset_id, dataset_name):
    try:
        response = requests.get(f"https://data.europa.eu/api/hub/search/datasets/{dataset_id}")
        status_code = response.status_code
        if status_code == 200:
            return dataset_id

        response = requests.get(f"https://data.europa.eu/api/hub/search/datasets/{dataset_name}")
        status_code = response.status_code
        if status_code == 200:
            return dataset_name

        return None
    except requests.RequestException:
        raise
