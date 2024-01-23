import json
import re
import urllib.parse

import assessment.probability_test
from file_manager import file_manager
from assessment.test import *


class I1(Metric):
    def __init__(self):
        super().__init__('I1', 'evaluating metadata language')

    def run_test(self):
        self.start_test()
        self.scored_point += 1
        # CATALOG USES CKAN API THAT GIVES BACK A JSON RESPONSE a formal, accessible, shared, and broadly
        # applicable language for knowledge representation.
        return self.end_test()


class I2(Metric):
    def __init__(self, payload):
        super().__init__('I2', 'evaluating vocabularies use')
        self.payload = payload

    def run_test(self):
        self.start_test()
        vocabularies = file_manager.open_file('vocabularies', 'json', True, 'set')

        self.max_point -= 1
        import re
        for payload_property in vocabularies:
            pattern = re.compile(fr'"{payload_property}"\s?:\s?"(\S+)"')
            matches = re.findall(pattern, json.dumps(self.payload))
            if matches:
                print(payload_property, matches)
                for match in matches:
                    self.max_point += 1
                    for vocabulary in vocabularies[payload_property]['vocabulary_format']:
                        if vocabulary in match:
                            self.scored_point += 1
                            break

            pattern2 = re.compile(fr'"{payload_property}"\s?:\s?(\[.+])')
            matches2 = re.findall(pattern2, json.dumps(self.payload))
            found = 0
            if matches2:
                for match in matches2:
                    self.max_point += 1
                    match = json.loads(match)
                    for vocabulary in vocabularies[payload_property]['vocabulary_format']:
                        for elem_match in match:
                            if vocabulary in elem_match:
                                self.scored_point += 1
                                found = 1
                                break
                        if found:
                            break

            pattern3 = re.compile(fr'"key"\s?:\s?"{payload_property}"\s?,\s?"value"\s?:\s?"\s?(\S+)\s?"\s?}}')
            matches3 = re.findall(pattern3, json.dumps(self.payload))

            if matches3:
                for match in matches3:
                    print(payload_property, matches3)

                    self.max_point += 1
                    for vocabulary in vocabularies[payload_property]['vocabulary_format']:
                        if vocabulary in match:
                            self.scored_point += 1
                            break

        self.max_point = max(1, self.max_point)
        return self.end_test()


class I3(Metric):
    def __init__(self, payload, url):
        super().__init__('I3', 'evaluating reference to other metadata')
        self.payload = payload
        self.url = url

    def run_test(self):
        self.start_test()
        text_payload = json.dumps(self.payload)
        print(self.url)

        domain_url = urllib.parse.urlparse(self.url).netloc
        print(urllib.parse.urlparse(self.url))

        url_found = re.findall(r'(https?://\S+)', text_payload)
        unique_url = []
        if url_found:
            for url in url_found:
                unique_url.append(url.split("//")[1].split("/")[0].split('"')[0].split(")")[0])
            self.max_point = len(url_found)
            self.info_test(f'found {str(len(url_found))} url', 1)
            unique_url = set(unique_url)
            print(unique_url)
            print(domain_url)

            for url in unique_url:
                if url.find(domain_url) == -1:
                    self.info_test(f'external link found: {url}', 2)
                    self.scored_point += 1

            ext_url = self.max_point - self.scored_point
            if ext_url < self.max_point:
                self.hint_test(f"only {self.scored_point} of {self.max_point} are outside of domain. "
                               f"Its good practice to do it as much as possible. (MIGHT NOT BE POSSIBLE)")
            if not ext_url:
                self.hint_test('No url found in metadata pointing outside of domain. '
                               'Its good practice to build linked data')

        return self.end_test()


class GuidelinesI(Metric):
    def __init__(self, payload):
        super().__init__('GUIDELINES I', '')
        self.payload = payload

    def run_test(self):
        self.start_test()
        self.scored_point += 1
        # CATALOG USES CKAN API THAT GIVES BACK A VALID UTF8 ENCODED RESPONSE

        perc_corr_date_format = assessment.probability_test.date_time_format(self.payload)
        if perc_corr_date_format[0] > 0:
            self.max_point += 1

            self.scored_point += perc_corr_date_format[1]/perc_corr_date_format[0]
        return self.end_test()


class InteroperabilityTest(Principle):
    def __init__(self, payload, url):
        self.payload = payload
        self.url = url
        metric_test = [I1(), I2(self.payload), I3(self.payload, self.url), GuidelinesI(self.payload)]

        super().__init__('I', metric_test)
