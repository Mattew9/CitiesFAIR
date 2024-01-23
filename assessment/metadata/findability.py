import copy
import json

import data_retriever
from file_manager import file_manager, FileManagerError
from assessment.metadata import support_f2, support_f4
from assessment.test import *


class F1(Metric):
    def __init__(self, payload):
        super().__init__( 'F1', 'finding dataset permanent URL')
        self.payload = payload

    def run_test(self):
        self.start_test()

        try:
            permalinks = file_manager.open_file('permanent_link', 'json', True, 'set')['formats']
            if "url" in self.payload["result"] and self.payload["result"]["url"]:
                for permalink in permalinks:
                    if self.payload["result"]["url"].find(permalink) == -1:
                        self.info_test('permalink not found', 1)
                    else:
                        self.info_test('permalink found', 1)
                        self.scored_point = 1
                        break

            elif "extras" in self.payload["result"]:
                for extra in self.payload["result"]["extras"]:
                    if extra["key"] == "uri":
                        for permalink in permalinks:
                            if extra["value"].find(permalink) == -1:
                                self.info_test('permalink not found', 1)
                            else:
                                self.info_test('permalink found', 1)
                                self.scored_point = 1
                                break

            if self.scored_point == 0:
                self.hint_test("No permalink found for the dataset. Add a permalink")

            return self.end_test()

        except FileManagerError as e:
            raise TestError('F1', e)


class F2(Metric):
    def __init__(self, payload, method, portal_url):
        super().__init__('F2', 'calculating metadata richness')
        self.payload = payload
        self.method = method
        self.portal_url = portal_url

    def run_test(self):
        self.start_test()
        # TODO:  implement RDF support
        # try:
        #     data_retriever.retrieve_dataset(self.portal_url + 'dataset/' + self.payload['result']['name'] + '.rdf')
        #
        #     g = file_manager.open_file(self.payload['result']['name'], 'rdf', k_path='temp')
        #
        #
        #
        #
        # except data_retriever.DataRetrieverError:
        #     pass
        try:
            prop_with_sub = support_f2.load_sup_link()
            prop_points = support_f2.load_point("result", self.method)
            schema_properties = support_f2.load_metadata_properties_schema(self.method)
            self.max_point = support_f2.load_max_point("result", self.method)
            metadata_properties = support_f2.normalizer(self.payload["result"])

            for _property in schema_properties:
                self.info_test(f"evaluating {str(_property)}", 1)
                weight = 1

                if _property in metadata_properties and metadata_properties[_property] != {} \
                        and metadata_properties[_property] is not None:
                    if _property in prop_with_sub:
                        self.info_test(f"{str(_property)} seems to have sub", 2)
                        weight = support_f2.find_weight(self.payload, _property, self.method)
                        self.info_test(f"{str(_property)} has weight: {weight}", 2)
                    self.info_test(f"points {str(prop_points[_property])} weight: {str(weight)}", 1)
                    self.scored_point += round(prop_points[_property] * weight, 2)
                else:
                    self.hint_test(f"Property {str(_property)} doesn't seem to be implemented")

        except file_manager.FileManagerError as e:
            raise TestError('F2', e)

        return self.end_test()


class F3(Metric):
    def __init__(self, payload):
        super().__init__("F3", "finding data identifier in the metadata")
        self.payload = payload

    def run_test(self):
        self.start_test()

        if "resources" not in self.payload["result"] or len(self.payload["result"]["resources"]) == 0:
            self.hint_test("distributions doesn't seem to be implemented, "
                           "data info should be implemented in the metadata")
            return self.end_test()

        self.max_point = len(self.payload["result"]["resources"])

        self.info_test(f"found {str(len(self.payload['result']['resources']))} distributions", 1)

        for idx, distribution in enumerate(self.payload["result"]["resources"]):
            if distribution["id"] is not None:
                self.info_test(f"for distribution {str(idx +1)} found id: {str(distribution['id'])}", 2)
                self.scored_point += 1
            else:
                self.hint_test(f"distribution {str(idx + 1)} doesn't seem to have an ID. "
                               f"ID should be implemented for all distributions")
        return self.end_test()


class F4(Metric):
    def __init__(self, payload, portal_url: str):
        super().__init__('F4', 'searching for metadata indexing')
        self.payload = payload
        if portal_url.endswith('ckan'):
            temp = portal_url.replace('ckan', '')
        elif portal_url.endswith('ckan/'):
            temp = portal_url.replace('ckan/', '')
        else:
            temp = portal_url

        if not temp.endswith('/'):
            self.portal_url = temp + '/'
        else:
            self.portal_url = temp

    def run_test(self):
        self.start_test()

        known_structures = [self.portal_url, self.portal_url + 'dataset/', self.portal_url + 'ckan/', self.portal_url + 'ckan/dataset/']

        for structure in known_structures:
            try:
                self.info_test(f"searching on DuckDuckGo", 1)
                if support_f4.duckduckgo_indexed(structure + self.payload['result']['name']):
                    self.info_test(f"found {self.portal_url + self.payload['result']['name']}", 2)
                    self.scored_point += 1
                    return self.end_test()

                else:
                    if support_f4.duckduckgo_indexed(structure + self.payload['result']['id']):
                        self.info_test(f"found {self.portal_url + self.payload['result']['name']}", 2)
                        self.scored_point += 1
                        return self.end_test()

                    else:
                        self.hint_test('resource not indexed')

            except support_f4.DuckDuckGoError as e:
                pass

        return self.end_test()


class GuidelinesF(Metric):
    def __init__(self, payload):
        super().__init__('GUIDELINES F', '')
        self.payload = payload

    def run_test(self):
        self.start_test()

        empty_fields = 0
        payload_text = json.dumps(self.payload)

        self.info_test("evaluating null values count in the metadata", 1)
        fields = payload_text.count('": "') + payload_text.count('": [') + payload_text.count('": {') + 1
        empty_fields += payload_text.count("null")
        empty_fields += payload_text.count("[]")
        empty_fields += payload_text.count("{}")
        self.info_test(f"found {str(empty_fields)} empty fields: {str(round(empty_fields / fields * 100, 2))}%", 2)
        self.scored_point += (fields - empty_fields) / fields

        self.info_test("evaluating important field presence in the metadata", 1)
        if 'tags' in self.payload.keys() and self.payload['result']['tags']:
            self.scored_point += 1
            self.info_test("keywords(tags) field found", 2)
        else:
            self.hint_test("keywords(tags) field not found")
        # TODO: find categories equivalent in CKAN response
        if 'categories' in self.payload.keys() and self.payload['result']['categories']:
            self.scored_point += 1
            self.info_test("categories field found", 2)
        else:
            self.hint_test("categories field not found")
        not_found = 1
        if 'extras' in self.payload.keys():
            for extra in self.payload['extras']:
                if not_found:
                    if extra['key'] == 'temporal_end' or extra['key'] == 'temporal_start':
                        if extra['value']:
                            self.scored_point += 1
                            self.info_test("temporal field found", 2)
                            not_found = 0
                            break
                        else:
                            self.hint_test("temporal field not filled")
            if not_found:
                self.hint_test("temporal field not found")
            for extra in self.payload['extras']:
                if not_found:
                    if extra['key'] == 'spatial_uri':
                        if extra['value']:
                            self.scored_point += 1
                            self.info_test("temporal field found", 2)
                            not_found = 0
                            break
                        else:
                            self.hint_test("temporal field not field")
            if not_found:
                self.hint_test("spatial field not found")

        else:
            self.hint_test("spatial field not found")
            self.hint_test("temporal field not found")

        return self.end_test()


class FindabilityTest(Principle):
    def __init__(self, payload, portal_url, method='DCAT_AP-IT'):
        self.payload = payload
        self.method = method
        self.portal_url = portal_url
        metric_test = [F1(self.payload), F2(self.payload, self.method, self.portal_url), F3(self.payload), F4(self.payload, self.portal_url), GuidelinesF(self.payload)]
        super().__init__('F', metric_test)


