import json
import datetime
import re
import threading
from typing import Optional

import requests

import cloud_manager
from constants import Constants
import geo_manager
import data_retriever
import database_manager
from event_bus import event_bus
from file_manager import file_manager
from assessment.metadata.findability import FindabilityTest as FindMeta
from assessment.metadata.accessibility import AccessibilityTest as AccMeta
from assessment.metadata.interoperability import InteroperabilityTest as IntMeta
from assessment.metadata.reusability import ReusabilityTest as ReuMeta
from assessment.data.findability import FindabilityTest as FindData
from assessment.data.accessibility import AccessibilityTest as AccData
from assessment.data.interoperability import InteroperabilityTest as IntData
from assessment.data.reusability import ReusabilityTest as ReuData
import assessment.test
import signal

def get_accrual(payload: dict) -> Optional[int]:
    """
    Extract from the metadata the accrual periodicity of the dataset
    :param payload: the dataset metadata
    :return: the accrual periodicity as an int number of days
    """
    if 'frequency' in payload['result']:
        frequency = payload['result']['frequency']
        for frequency_value in Constants.FREQUENCIES.keys():
            if frequency == frequency_value:
                return Constants.FREQUENCIES[frequency_value]

    return None


def is_modified(dataset_id: str, payload: dict, cloud_service: cloud_manager.Cloud) -> bool:
    """
    Check if the dataset has been modified since last evaluation
    :param dataset_id: the id of the dataset
    :param payload: the new payload of the dataset
    :param cloud_service: the cloud service instance
    :return: True if the dataset has been modified, False otherwise
    """
    old_payload_id = cloud_service.search_folder_and_get_latest_file(dataset_id, 'ckan')
    if old_payload_id:
        old_payload_file_id = cloud_service.download_file(old_payload_id)
        old_payload = file_manager.open_file(old_payload_file_id, 'txt', True, 'temp')
        file_manager.delete_file(old_payload_file_id, 'txt')

        if json.dumps(old_payload) != json.dumps(payload):
            for key in old_payload['result'].keys():
                if key != 'metadata_modified':
                    if payload['result'][key] != old_payload['result'][key]:
                        return True
    return False


def find_url(poss_url):
    try:
        for url in poss_url:
            if requests.get(url).status_code == 200:
                return url
    except requests.RequestException as e:
        print(e)


class Evaluator:
    def __init__(self):
        self.running = True
        event_bus.subscribe(self)

    def start_eval(self, cloud_service: cloud_manager.Cloud, database: database_manager.DatabaseManager, datasets,
                   portal_instance: data_retriever.DataRetriever, locator: geo_manager.CoorFinder):
        length = len(datasets)

        event = {
            'type': 'update_text_toplevel',
            'text': f'{length} Dataset to Update Found',
            'text1': '',
            'text2': '',
            'text3': ''

        }
        event_bus.publish(event)

        for idx, dataset_id in enumerate(datasets):
            if self.running:

                known_holders = database.get_holders_list()
                known_datasets = database.get_dataset_list()
                known_categories = database.get_category_list()

                try:
                    payload = portal_instance.get_dataset(dataset_id)
                    curr_holder = payload['result']['holder_identifier']

                    event = {
                        'type': 'update_text_toplevel',
                        'text': f'Updating Dataset {idx + 1} of {length}: {dataset_id}',
                        'text1': 'Checking Database Holder',
                        'text2': '',
                        'text3': ''

                    }
                    event_bus.publish(event)
                    if curr_holder not in known_holders:
                        event = {
                            'type': 'update_text_toplevel',
                            'text': None,
                            'text1': 'Checking Database Holder',
                            'text2': 'New Holder Found',
                            'text3': 'Saving'

                        }
                        event_bus.publish(event)

                        database.save_holder(curr_holder, payload['result']['holder_name'],
                                             (locator.get_location(curr_holder) if locator else None),
                                             (locator.get_prov(curr_holder) if locator else None),
                                             (locator.get_city(curr_holder) if locator else None))

                    event = {
                        'type': 'update_text_toplevel',
                        'text': None,
                        'text1': 'Checking Dataset',
                        'text2': '',
                        'text3': ''

                    }
                    event_bus.publish(event)

                    if dataset_id not in known_datasets:
                        event = {
                            'type': 'update_text_toplevel',
                            'text': None,
                            'text1': 'Checking Dataset',
                            'text2': 'New Dataset Found',
                            'text3': 'Saving'

                        }
                        event_bus.publish(event)

                        database.save_dataset(dataset_id, curr_holder, get_accrual(payload),
                                              (payload['name'] if 'name' in payload else None))

                    dataset_categories = []

                    event = {
                        'type': 'update_text_toplevel',
                        'text': None,
                        'text1': 'Checking Database Category',
                        'text2': ' ',
                        'text3': ' '

                    }
                    event_bus.publish(event)

                    if 'groups' in payload['result'] and payload['result']['groups']:
                        for category in payload['result']['groups']:
                            dataset_categories.append(category['name'])

                    for category in dataset_categories:
                        if category not in known_categories:
                            event = {
                                'type': 'update_text_toplevel',
                                'text': None,
                                'text1': 'Checking Database Category',
                                'text2': f'New Category Found: {category}',
                                'text3': 'Saving'

                            }
                            event_bus.publish(event)
                            database.save_category(category)

                    known_dataset_categories = database.get_category_for_dataset(dataset_id)

                    for category in dataset_categories:
                        if category not in known_dataset_categories:
                            event = {
                                'type': 'update_text_toplevel',
                                'text': None,
                                'text1': 'Checking Database Category',
                                'text2': f'New Dataset-Category Relationship Found for: {category}',
                                'text3': 'Saving'

                            }
                            event_bus.publish(event)
                            database.save_category_dataset_relation(dataset_id, category)

                    if dataset_id not in known_datasets:
                        event = {
                            'type': 'update_text_toplevel',
                            'text': None,
                            'text1': 'Dataset not Assessed -> Assessing',
                            'text2': ' ',
                            'text3': ' '

                        }

                        event_bus.publish(event)

                    is_modified_ = None
                    cloud_payload_id = None
                    if cloud_service:
                        is_modified_ = is_modified(dataset_id, payload, cloud_service)

                        file_name = 'ckan-' + dataset_id + str(datetime.datetime.now()).replace(' ', '-')
                        file_name = re.sub(r'[^\w\-_.]', '-', file_name)

                        file_manager.write_file(file_name + '.txt', payload, file_manager.temp_file_path, True)
                        cloud_payload_id = cloud_service.upload(file_name, dataset_id)

                        file_manager.delete_file(file_name)

                    portal_url = portal_instance.get_url() + '/'
                    temp_url = portal_url + 'dataset/'
                    poss_url = [
                        portal_url + payload['result']['name'],
                        portal_url + payload['result']['id'],
                        temp_url + payload['result']['name'],
                        temp_url + payload['result']['id']
                    ]


                    page_url = find_url(poss_url)
                    event = {
                        'type': 'update_text_toplevel',
                        'text': None,
                        'text1': 'Dataset not Assessed -> Assessing',
                        'text2': 'Assessing Metadata',
                        'text3': None

                    }

                    event_bus.publish(event)

                    assessment_id = database.new_assessment(dataset_id)

                    metadata = assessment.test.Structure("METADATA", [
                        FindMeta(payload, page_url),
                        AccMeta(payload),
                        IntMeta(payload, portal_instance.get_url()),
                        ReuMeta(payload)
                    ])

                    metadata_assessment = metadata.run()
                    database.save_metadata_assessment(assessment_id, metadata_assessment,
                                                      (is_modified_ if cloud_service else None),
                                                      (cloud_payload_id if cloud_service else None))

                    event = {
                        'type': 'update_text_toplevel',
                        'text': None,
                        'text1': None,
                        'text2': 'Assessing Data',
                        'text3': None

                    }

                    event_bus.publish(event)
                    if 'resources' in payload['result'] and payload['result']['resources']:
                        try:
                            for idx_, resource in enumerate(payload['result']['resources']):
                                event = {
                                    'type': 'update_text_toplevel',
                                    'text': None,
                                    'text1': None,
                                    'text2': None,
                                    'text3': f'Distribution {idx_ + 1} of {len(payload["result"]["resources"])}: '
                                             f'{resource["name"]}'

                                }

                                event_bus.publish(event)
                                resource_payload = resource
                                resource_file = None
                                try:
                                    if not str(resource_payload['url']).endswith('rss.xml'):
                                        if data_retriever.get_dataset_distribution(resource_payload['url']):
                                            resource_file = data_retriever.retrieve_dataset(resource_payload['url'])
                                except TimeoutError:
                                    pass
                                except requests.exceptions.SSLError as e:
                                    pass
                                except requests.exceptions.RequestException as e:
                                    pass

                                # TODO: aggiungere cloud per file
                                is_file_modified = None
                                resource_file_id = None
                                data = assessment.test.Structure(f'Data for Resource: {resource_payload["id"]}',
                                                                 [FindData(resource_payload),
                                                                  AccData(resource_payload),
                                                                  IntData(resource_payload, resource_file),
                                                                  ReuData(payload, resource_payload, resource_file)
                                                                  ])
                                data_assessment = data.run()

                                database.save_data_assessment(assessment_id, resource_payload['id'],
                                                              (resource_payload[
                                                                   'name'] if 'name' in resource_payload else None),
                                                              data_assessment,
                                                              (is_file_modified if cloud_service else None),
                                                              (resource_file_id if cloud_service else None))
                                if resource_file:
                                    file_manager.delete_file(resource_file[:resource_file.rfind(".")],
                                                             resource_file.split('.')[-1])
                        except assessment.test.TestError as e:
                            print(e)
                except KeyError:
                    pass
                except assessment.test.TestError as e:
                    print(e)

        event = {
            'type': 'update_done_toplevel'
        }
        event_bus.publish(event)

    def handle_event(self, event):
        if event['type'] == 'c_window':
            self.running = False
