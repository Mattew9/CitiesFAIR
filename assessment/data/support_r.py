import json
import requests


def retrieve_mqa_point(dataset_id, distribution_name):
    try:
        response = requests.get(f"https://data.europa.eu/api/mqa/cache/datasets/{dataset_id}/distributions", headers={'Accept-Language': 'it'})
        score = 0

        if response.status_code == 200:
            payload = json.loads(response.text)
            corr_distribution = None

            for distribution in payload['result']['results']:
                if distribution[0]['info']['distribution-title'] == distribution_name:

                    corr_distribution = distribution[0]

            if corr_distribution:
                if corr_distribution['contextuality'][0]['byteSizeAvailability']: score += 1
                if corr_distribution['contextuality'][1]['rightsAvailability']: score += 1
                if corr_distribution['contextuality'][2]['dateModifiedAvailability']: score += 1
                if corr_distribution['contextuality'][3]['dateIssuedAvailability']: score += 1

                if corr_distribution['accessibility'][0]['downloadUrlAvailability']: score += 1
                if corr_distribution['accessibility'][1]['accessUrlStatusCode'] == 200: score += 1
                if corr_distribution['accessibility'][2]['downloadUrlStatusCode'] == 200: score += 1

                if corr_distribution['interoperability'][0]['formatAvailability'] == 200: score += 1
                if corr_distribution['interoperability'][1]['mediaTypeAvailability'] == 200: score += 1
                if corr_distribution['interoperability'][2]['formatMediaTypeVocabularyAlignment'] == 200: score += 1

                if corr_distribution['reusability'][0]['licenceAvailability']: score += 1

                return score

        return 0

    except requests.RequestException:
        raise
