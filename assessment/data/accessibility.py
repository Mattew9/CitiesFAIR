import requests

import data_retriever
from assessment.test import *


class A1(Metric):
    def __init__(self):
        super().__init__('A1', 'evaluating communication protocol')

    def run_test(self):
        self.start_test()
        self.scored_point += 1
        # CKAN API ARE STANDARDIZED PROTOCOLS FOR METADATA RETRIVAL
        return self.end_test()


class A11(Metric):
    def __init__(self):
        super().__init__('A1.1', 'evaluating communication protocol')

    def run_test(self):
        self.start_test()
        self.scored_point += 1
        # CKAN API ARE STANDARDIZED PROTOCOLS FOR METADATA RETRIVAL that are open, free,
        # and universally implementable
        return self.end_test()


class A12(Metric):
    def __init__(self):
        super().__init__('A1.2', 'evaluating communication protocol')

    def run_test(self):
        self.start_test()
        self.scored_point += 1
        # CKAN API ARE STANDARDIZED PROTOCOLS FOR METADATA RETRIVAL that allow for authorization
        # protocols for the nature of the data itself
        return self.end_test()


class A2(Metric):
    def __init__(self):
        super().__init__('A2', 'evaluating communication protocol')

    def run_test(self):
        self.start_test()
        self.scored_point += 1
        # CKAN TAKE HOLD OF THE METADATA EVEN IF A DATASET IS OUT OF REACH
        return self.end_test()


class GuidelinesA(Metric):
    def __init__(self, resource_payload: dict):
        super().__init__('GUIDELINES A', '')
        self.resource_payload = resource_payload

    def run_test(self):
        self.start_test()
        self.max_point += 1

        if 'url' in self.resource_payload and self.resource_payload['url']:
            try:
                status = data_retriever.get_dataset_distribution(self.resource_payload['url'])

                max_point_found = 0
                if status in range(200, 400) or status == 401:
                    self.info_test('url working', 3)
                    max_point_found = max(1, max_point_found)
                    if status != 401:
                        self.info_test('url accessible', 3)
                        max_point_found = max(2, max_point_found)
            except requests.RequestException as e:
                raise TestError('GUIDELINES A', e)
            except Exception as e:
                raise TestError('GUIDELINES A', e)

            self.scored_point += max_point_found
        else:
            self.hint_test("file url doesn't seem to be implemented")

        return self.end_test()


class AccessibilityTest(Principle):
    def __init__(self, resource_payload):
        self.resource_payload = resource_payload

        metric_test = [A1(), A11(), A12(), A2(), GuidelinesA(self.resource_payload)]
        super().__init__('A', metric_test)
