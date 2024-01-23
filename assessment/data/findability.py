import json

from file_manager import file_manager
from assessment.metadata import support_f2, support_f4
from assessment.test import *


class F1(Metric):
    def __init__(self, resource_payload):
        super().__init__('F1', 'finding resource permanent URL')
        self.resource_payload = resource_payload

    def run_test(self):
        self.start_test()

        try:
            permalinks = file_manager.open_file('permanent_link', 'json', True, 'set')['formats']
            if "url" in self.resource_payload and self.resource_payload["url"]:
                for permalink in permalinks:
                    if self.resource_payload["url"].find(permalink) == -1:
                        self.info_test(f'permalink not found: {permalink}', 1)
                    else:
                        self.info_test(f'permalink found: {permalink}', 1)
                        self.scored_point = 1
                        break
            if self.scored_point == 0:
                self.hint_test("No permalink found for resource. Add a permalink")

            return self.end_test()

        except file_manager.LoaderFileError as e:
            raise TestError('F1', e)


class F2(Metric):
    def __init__(self, resource_payload: dict, method: str):
        super().__init__('F2', 'calculating metadata richness for data resource')
        self.resource_payload = resource_payload
        self.method = method

    def run_test(self):
        self.start_test()
        try:
            prop_with_sub = support_f2.load_sup_link()
            prop_points = support_f2.load_point("resources", self.method)
            schema_properties = support_f2.load_metadata_properties_schema(self.method, "resources")
            self.max_point = support_f2.load_max_point("resources", self.method)

            metadata_properties = support_f2.normalizer(self.resource_payload)

            for _property in schema_properties:
                self.info_test(f"evaluating {str(_property)}", 1)
                weight = 1

                if _property in metadata_properties and metadata_properties[_property] != {} \
                        and metadata_properties[_property] is not None:
                    if _property in prop_with_sub:
                        self.info_test(f"{str(_property)} seems to have sub", 2)
                        weight = support_f2.find_weight(self.resource_payload, _property, self.method)
                        self.info_test(f"{str(_property)} has weight: {weight}", 2)
                    self.info_test(f"points {str(prop_points[_property])} weight: {str(weight)}", 1)
                    self.scored_point += round(prop_points[_property] * weight, 2)
                else:
                    self.hint_test(f"Property {str(_property)} doesn't seem to be implemented")

        except file_manager.LoaderFileError as e:
            raise TestError('F2', e)

        return self.end_test()


class F3(Metric):
    def __init__(self, resource_payload: dict):
        super().__init__('F3', 'finding data identifier')
        self.resource_payload = resource_payload

    def run_test(self):
        self.start_test()

        if "id" not in self.resource_payload:
            self.hint_test("distributions doesn't seem to be implemented, "
                           "data info should be implemented in the metadata")
            return self.end_test()
        if self.resource_payload['id']:
            self.info_test(f"for distribution found id: {str(self.resource_payload['id'])}", 2)
            self.scored_point += 1
        else:
            self.hint_test(f"distribution doesn't seem to have an ID. "
                           f"ID should be implemented for all distributions")
        return self.end_test()


class F4(Metric):
    def __init__(self, resource_payload):
        super().__init__('F4', 'searching for metadata indexing')
        self.resource_payload = resource_payload

    def run_test(self):
        self.start_test()

        try:
            self.info_test(f"searching on DuckDuckGo", 1)
            if 'url' in self.resource_payload and self.resource_payload['url']:
                if support_f4.duckduckgo_indexed(self.resource_payload['url']):
                    self.info_test(f"found {self.resource_payload['url']}", 2)
                    self.scored_point += 1
                else:
                    self.hint_test('resource not indexed')
            else:
                self.hint_test('resource not indexed')

            return self.end_test()

        except support_f4.DuckDuckGoError as e:
            raise TestError('F4', e)


class GuidelinesF(Metric):
    def __init__(self, resource_payload: dict):
        super().__init__('GUIDELINES F', '')
        self.resource_payload = resource_payload

    def run_test(self):
        self.start_test()

        empty_fields = 0
        payload_text = json.dumps(self.resource_payload)

        self.info_test("evaluating null values count in the resource metadata", 1)
        fields = payload_text.count('": "') + payload_text.count('": [') + payload_text.count('": {') + 1
        empty_fields += payload_text.count("null")
        empty_fields += payload_text.count("[]")
        empty_fields += payload_text.count("{}")
        self.info_test(f"found {str(empty_fields)} empty fields: {str(round(empty_fields / fields * 100, 2))}%", 2)
        self.scored_point += (fields - empty_fields) / fields

        return self.end_test()


class FindabilityTest(Principle):
    def __init__(self, resource_payload: dict, method: str = 'DCAT_AP-IT'):
        self.resource_payload = resource_payload
        self.method = method
        metric_test = [F1(self.resource_payload), F2(self.resource_payload, self.method), F3(self.resource_payload),
                       F4(self.resource_payload), GuidelinesF(self.resource_payload)]
        super().__init__('F', metric_test)
