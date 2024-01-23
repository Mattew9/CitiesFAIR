import requests

from file_manager import file_manager
from assessment.metadata import support_r
from assessment.data.support_r import retrieve_mqa_point
from assessment.test import *
from assessment.data.file_specific.xml import sp_xml


class R11(Metric):
    def __init__(self, resource_payload: dict):
        super().__init__('R1.1', 'searching for license in resource metadata')
        self.resource_payload = resource_payload

    def run_test(self):
        self.start_test()
        vocabularies = file_manager.open_file('vocabularies', 'json', True, 'set')

        self.max_point = 2
        found = 0
        if "license" in self.resource_payload and self.resource_payload["license"] is not None:
            self.info_test(f"for distribution found license", 2)
            self.scored_point += 1
            for license_format in vocabularies["license"]["vocabulary_format"]:
                if str(self.resource_payload['license']).find(license_format) != -1:
                    self.scored_point += 1
                    self.info_test("license respect vocabulary", 3)
                    found = 1
            if not found:
                self.hint_test(f"license for distribution doesn't respect vocabulary")

        elif "license_type" in self.resource_payload and self.resource_payload["license_type"] is not None:
            self.info_test(f"for distribution found license", 2)
            self.scored_point += 1
            for license_format in vocabularies["license"]["vocabulary_format"]:
                if str(self.resource_payload['license_type']).find(license_format) != -1:
                    self.scored_point += 1
                    self.info_test("license respect vocabulary", 3)
                    found = 1
            if not found:
                self.hint_test(f"license for distribution doesn't respect vocabulary")
        else:
            self.hint_test(f"license not found")

        return self.end_test()


class R12(Metric):
    def __init__(self, resource_payload):
        super().__init__('R1.2', 'searching for provenance in metadata')

    def run_test(self):
        self.start_test()
        # TODO: does CKAN support provenance information in the resource?

        return self.end_test()


class R13(Metric):
    def __init__(self, payload: dict, resource_payload: dict):
        super().__init__('R1.3', 'finding community score')
        self.resource_payload = resource_payload
        dataset_name = ''
        dataset_id = ''
        if 'name' in payload['result']:
            dataset_name = payload['result']['name']
        if 'id' in payload['result']:
            dataset_id = payload['result']['id']
        try:
            self.europa_id = support_r.find_europa_id(dataset_name, dataset_id)
        except requests.RequestException as e:
            raise TestError('R1.3', e)

    def run_test(self):
        self.start_test()
        if self.europa_id:
            self.max_point = 10

            try:
                self.scored_point = retrieve_mqa_point(self.europa_id, self.resource_payload['name'])

                if self.scored_point < self.max_point:
                    self.hint_test('resource score is not max on community guidelines')

                self.hint_test('dataset might not be presente in community portal (data.europa.eu)')
            except requests.RequestException as e:
                raise TestError('R1.3', e)

        return self.end_test()


class GuidelinesR(Metric):
    def __init__(self, resource_payload: dict, file_name: str):
        super().__init__('GUIDELINES R', '')
        self.resource_payload = resource_payload
        self.file_name = file_name

    def run_test(self):
        self.start_test()
        self.info_test('compliance to community standard always true for ckan', 1)
        self.scored_point += 1

        self.info_test('searching for size in metadata', 1)
        self.max_point += 1
        if "size" in self.resource_payload and self.resource_payload["size"]:
            self.info_test('size found', 2)
            self.scored_point += 1
        else:
            self.hint_test('size not implemented')

        if self.file_name:
            match self.file_name.split('.')[-1]:
                case 'xml':
                    self.max_point += 1
                    if sp_xml.has_declaration(self.file_name):
                        self.scored_point += 1
                        self.info_test("file has xml declaration", 1)
                    else:
                        self.hint_test("file doesn't seem to have an xml declaration")

                    self.max_point += 1
                    if sp_xml.check_escape_use(self.file_name):
                        self.scored_point += 1
                        self.info_test("file seems to use correct escape", 1)
                    else:
                        self.hint_test("file doesn't seem to use correct escape characters")
                    # TODO: how to verify camelCase or PascalCase

        return self.end_test()


class ReusabilityTest(Principle):
    def __init__(self, payload, resource_payload, file_name):
        self.payload = payload
        self.resource_payload = resource_payload
        self.file_name = file_name
        metric_test = [R11(self.resource_payload), R12(self.resource_payload), R13(self.payload, self.resource_payload), GuidelinesR(self.resource_payload, self.file_name)]

        super().__init__('R', metric_test)