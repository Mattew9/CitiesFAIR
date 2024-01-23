from file_manager import file_manager
from assessment.metadata import support_r
from assessment.metadata.support_r import retrieve_mqa_point
from assessment.test import *


class R11(Metric):
    def __init__(self, payload):
        super().__init__('R1.1', 'searching for license in metadata')
        self.payload = payload

    def run_test(self):
        self.start_test()
        vocabularies = file_manager.open_file('vocabularies', 'json', True, 'set')

        self.max_point = 2
        if "license_url" in self.payload['result'] and self.payload['result']["license_url"] is not None:
            self.info_test(f"license found", 2)
            self.scored_point += 1
            for license_format in vocabularies["license_type"]["vocabulary_format"]:
                if str(self.payload['result']['license_url']).find(license_format) != -1:
                    self.scored_point += 1
                    self.info_test("license respect vocabulary", 3)
                else:
                    self.hint_test(f"license for distribution doesn't respect vocabulary")

        elif "license_id" in self.payload['result'] and self.payload['result']["license_id"] is not None:
            self.info_test(f"license found", 2)
            self.scored_point += 1
            for license_format in vocabularies["license_type"]["vocabulary_format"]:
                if str(self.payload['result']['license_id']).find(license_format) != -1:
                    self.scored_point += 1
                    self.info_test("license respect vocabulary", 3)
                else:
                    self.hint_test(f"license for distribution doesn't respect vocabulary")
        else:
            self.hint_test(f"license not found")

        return self.end_test()


class R12(Metric):
    def __init__(self, payload):
        super().__init__('R1.2', 'searching for provenance in metadata')
        self.payload = payload

    def run_test(self):
        self.start_test()
        found = 0
        if 'extras' in self.payload['result']:
            for extra in self.payload['result']['extras']:
                if extra['key'] == 'provenance' and extra['value']:
                    self.info_test('found provenance', 1)
                    self.scored_point += 1
                    found = 1
                    break

            if not found:
                for extra in self.payload['result']['extras']:
                    # TODO: find creator equivalent in CKAN response
                    if (extra['key'] == 'publisher_uri' and extra['value']) or (extra['key'] == 'creator'
                                                                                and extra['value']):
                        self.hint_test('provenance not found, but found publisher/creator')
                        found = 1
                        self.scored_point += 0.5
                        break
        if not found:
            self.hint_test('provenance not found')

        return self.end_test()


class R13(Metric):
    def __init__(self, payload):
        super().__init__('R1.3', 'finding community score')
        dataset_name = ''
        dataset_id = ''
        if 'name' in payload['result']:
            dataset_name = payload['result']['name']
        if 'id' in payload['result']:
            dataset_id = payload['result']['id']

        self.europa_id = support_r.find_europa_id(dataset_name, dataset_id)

    def run_test(self):
        self.start_test()
        if self.europa_id:
            self.max_point = 405

            self.scored_point = retrieve_mqa_point(self.europa_id)

            if self.scored_point < self.max_point:
                self.hint_test('dataset score is not max on community guidelines')

        self.hint_test('dataset might not be presente in community portal (data.europa.eu)')
        return self.end_test()


class GuidelinesR(Metric):
    def __init__(self, payload):
        super().__init__('GUIDELINES R', '')
        self.payload = payload

    def run_test(self):
        self.start_test()
        self.info_test('compliance to community standard always true for ckan', 1)
        self.scored_point += 1

        self.info_test('searching for size in metadata', 1)
        self.max_point += 1
        for distribution in self.payload["result"]["resources"]:
            if "size" in distribution and distribution["size"] is not None:
                self.info_test('size found', 2)
                self.scored_point += 1
            else:
                self.hint_test('size not implemented')

        return self.end_test()


class ReusabilityTest(Principle):
    def __init__(self, payload):
        self.payload = payload
        metric_test = [R11(self.payload), R12(self.payload), R13(self.payload), GuidelinesR(self.payload)]

        super().__init__('R', metric_test)