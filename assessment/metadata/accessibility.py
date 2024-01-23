import requests

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
    def __init__(self, payload):
        super().__init__('GUIDELINES A', '')
        self.payload = payload

    def run_test(self):
        self.start_test()
        if "resources" not in self.payload["result"] or self.payload["result"]["resources"] == [1]:
            self.hint_test("distributions doesn't seem to be implemented, data info should be implemented "
                           "in the metadata")
            return self.end_test()

        self.max_point = len(self.payload["result"]["resources"]) * 3
        self.info_test(f"found {str(len(self.payload['result']['resources']))} distributions", 1)

        for idx, distribution in enumerate(self.payload["result"]["resources"]):
            self.info_test(f"distribution {str(idx + 1)}", 2)
            if "url" in distribution \
                    and distribution["url"] is not None \
                    and distribution["url"] != []:
                self.info_test('found download url', 3)
                self.scored_point += 1
                max_point_found = 0

                if isinstance(distribution["url"], str):
                    urls = [distribution["url"]]
                else:
                    urls = distribution["url"]

                for url in urls:
                    try:
                        response = requests.get(url)
                        if response.status_code in range(200, 400) or response.status_code == 401:
                            self.info_test('url working', 3)
                            self.scored_point += 1
                            if response.status_code != 401:
                                self.info_test('url accessible', 3)
                                self.scored_point += 1
                                break
                            else:
                                self.hint_test("distribution {distribution['id']} -> url found but not accessible")
                        else:
                            self.hint_test("distribution {distribution['id']} -> url not working")

                    except requests.RequestException as e:
                        raise TestError('GUIDELINES A', e)
                    except Exception as e:
                        raise TestError('GUIDELINES A', e)
            else:
                self.hint_test("distribution {distribution['id']} -> url not found")
        return self.end_test()


class AccessibilityTest(Principle):
    def __init__(self, payload):
        self.payload = payload

        metric_test = [A1(), A11(), A12(), A2(), GuidelinesA(self.payload)]
        super().__init__('A', metric_test)
